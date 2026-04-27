package com.kubrik.mex.cli;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.migration.JobId;
import com.kubrik.mex.migration.MigrationService;
import com.kubrik.mex.migration.events.CollectionProgress;
import com.kubrik.mex.migration.events.JobEvent;
import com.kubrik.mex.migration.events.JobStatus;
import com.kubrik.mex.migration.events.ProgressSnapshot;
import com.kubrik.mex.migration.gate.PreconditionGate;
import com.kubrik.mex.migration.profile.ProfileCodec;
import com.kubrik.mex.migration.sink.PluginLoader;
import com.kubrik.mex.migration.spec.ErrorPolicy;
import com.kubrik.mex.migration.spec.ExecutionMode;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.migration.spec.VerifySpec;
import com.kubrik.mex.store.AppPaths;
import com.kubrik.mex.store.ConnectionStore;
import com.kubrik.mex.store.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Headless migration CLI (UX-5). Loads a profile YAML (or resumes an existing job), runs a
 *  migration to completion, streams events to stdout as JSON lines, and exits with a shell
 *  status that maps onto the terminal {@link JobStatus}: {@code 0} for COMPLETED or
 *  COMPLETED_WITH_WARNINGS, {@code 1} for FAILED, {@code 2} for CANCELLED, {@code 64} for
 *  argument / profile errors, {@code 65} for connection / service errors.
 *  <p>
 *  Shares the SQLite store, connection store, and plugin directory with the GUI — running
 *  the CLI and GUI simultaneously against the same user home is only safe in read-only
 *  scenarios; concurrent writes rely on SQLite's default WAL and may contend. */
@Command(
        name = "mongo-explorer-migrate",
        mixinStandardHelpOptions = true,
        version = "mongo-explorer-migrate v2.0.0",
        description = "Run a Mongo Explorer migration profile headlessly. Events stream to stdout as JSON lines.")
public final class MigrateCli implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MigrateCli.class);

    @Option(names = {"-p", "--profile"},
            description = "Path to a profile YAML (see docs/v2/v2.0/profile-yaml-sinks.md + MVP profile schema).")
    Path profilePath;

    @Option(names = {"-r", "--resume"},
            description = "Resume a previously-interrupted job by its ID. Mutually exclusive with --profile.")
    String resumeJobId;

    @Option(names = "--dry-run",
            description = "Force ExecutionMode.DRY_RUN regardless of what the profile sets. "
                    + "Reads + transforms run; no writes hit the target.")
    boolean dryRun;

    @Option(names = "--verify",
            description = "Force VerifySpec.verifyCounts=true on the loaded profile.")
    boolean verify;

    @Option(names = "--environment",
            description = "Override Options.environment (VER-8). Takes precedence over the profile value.")
    String environment;

    @Option(names = "--timeout-seconds",
            description = "Hard wall-clock timeout for the job; the CLI exits 124 if exceeded. 0 = unbounded.")
    long timeoutSeconds = 0;

    /** For tests — set to redirect stdout/stderr without touching {@link System}. */
    PrintWriter out = new PrintWriter(System.out, true);
    PrintWriter err = new PrintWriter(System.err, true);

    @Override
    public void run() {
        int exit = execute();
        // picocli swallows non-zero returns from Runnable; call exit directly so shell scripts
        // can branch on the status.
        System.exit(exit);
    }

    /** Separated from {@link #run()} so tests can assert the exit code without killing the JVM. */
    public int execute() {
        if (profilePath == null && resumeJobId == null) {
            err.println("error: one of --profile or --resume is required.");
            return 64;
        }
        if (profilePath != null && resumeJobId != null) {
            err.println("error: --profile and --resume are mutually exclusive.");
            return 64;
        }

        Database db = null;
        ConnectionManager manager = null;
        try {
            db = new Database();
            PluginLoader.loadFrom(AppPaths.pluginsDir());
            ConnectionStore store = new ConnectionStore(db);
            EventBus bus = new EventBus();
            manager = new ConnectionManager(store, bus, new Crypto());
            this.cliManager = manager;
            MigrationService service = new MigrationService(
                    manager, store, db, bus, new PreconditionGate(store, manager));

            ObjectMapper json = new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL);

            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<JobStatus> terminal = new AtomicReference<>();
            bus.onJob(e -> {
                writeEvent(json, e);
                if (e instanceof JobEvent.StatusChanged sc && sc.newStatus().isTerminal()) {
                    terminal.set(sc.newStatus());
                    done.countDown();
                }
            });

            JobId jobId = resumeJobId != null
                    ? resume(service)
                    : launch(service);

            if (jobId == null) return 65;

            boolean finished = timeoutSeconds > 0
                    ? done.await(timeoutSeconds, TimeUnit.SECONDS)
                    : await(done);
            if (!finished) {
                err.println("error: job " + jobId + " did not reach a terminal state within "
                        + timeoutSeconds + "s — cancelling.");
                try { service.cancel(jobId); } catch (Exception ignored) {}
                return 124;
            }
            return exitFor(terminal.get());
        } catch (IllegalArgumentException | IllegalStateException e) {
            err.println("error: " + e.getMessage());
            return 65;
        } catch (Exception e) {
            log.error("CLI failed", e);
            err.println("error: " + e.getMessage());
            return 65;
        } finally {
            out.flush();
            err.flush();
            try { if (manager != null) manager.closeAll(); } catch (Exception ignored) {}
            try { if (db != null) db.close(); } catch (Exception ignored) {}
        }
    }

    private JobId launch(MigrationService service) throws Exception {
        if (!Files.exists(profilePath)) {
            err.println("error: profile file not found: " + profilePath);
            return null;
        }
        String yaml = Files.readString(profilePath);
        MigrationSpec spec = new ProfileCodec().fromYaml(yaml);
        spec = applyOverrides(spec);

        // The CLI shares ConnectionManager with the GUI but never had
        // a connect() prelude — UI flows trigger it via tab activation.
        // Without it, preflight sees both connections as "not active"
        // and fails before the spec runs.
        connectAndWait(spec.source().connectionId(), "source");
        connectAndWait(spec.target().connectionId(), "target");

        return dryRun ? service.startDryRun(spec) : service.start(spec);
    }

    /** Drive a connect → wait-for-CONNECTED handshake on the shared
     *  manager so {@code PreflightChecker.manager.service(id)} returns
     *  a live MongoService. 30 s budget — generous for a localhost
     *  testcontainer, comfortable for a real Atlas endpoint behind
     *  TLS handshake. */
    private void connectAndWait(String connectionId, String role) {
        if (cliManager == null || connectionId == null) return;
        if (cliManager.state(connectionId).status()
                == com.kubrik.mex.model.ConnectionState.Status.CONNECTED) {
            return;
        }
        cliManager.connect(connectionId);
        long deadline = System.currentTimeMillis() + 30_000L;
        // After a fresh connect() call we should never see DISCONNECTED
        // unless someone disconnected the connection mid-flight (race
        // with another caller, manual SIGINT, etc.). Exit immediately
        // instead of spinning until the 30 s deadline.
        boolean sawConnecting = false;
        while (System.currentTimeMillis() < deadline) {
            var st = cliManager.state(connectionId).status();
            if (st == com.kubrik.mex.model.ConnectionState.Status.CONNECTED) return;
            if (st == com.kubrik.mex.model.ConnectionState.Status.ERROR) {
                throw new IllegalStateException(role + " connection '" + connectionId
                        + "' failed to connect: "
                        + cliManager.state(connectionId).lastError());
            }
            if (st == com.kubrik.mex.model.ConnectionState.Status.CONNECTING) {
                sawConnecting = true;
            } else if (st == com.kubrik.mex.model.ConnectionState.Status.DISCONNECTED
                    && sawConnecting) {
                // We saw CONNECTING transition back to DISCONNECTED
                // — that's a regression, not a startup race.
                throw new IllegalStateException(role + " connection '" + connectionId
                        + "' transitioned back to DISCONNECTED during connect");
            }
            try { Thread.sleep(50); }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new IllegalStateException(role + " connection '" + connectionId
                + "' did not reach CONNECTED within 30s");
    }

    /** Captured during run() so launch() can call connectAndWait. */
    private com.kubrik.mex.core.ConnectionManager cliManager;

    private JobId resume(MigrationService service) {
        return service.resume(JobId.of(resumeJobId));
    }

    /** Applies the CLI-supplied overrides (`--verify`, `--environment`) to an already-loaded
     *  spec. {@code --dry-run} is applied via {@link MigrationService#startDryRun} instead,
     *  which drives the same {@link ExecutionMode#DRY_RUN} flip the engine would see. */
    MigrationSpec applyOverrides(MigrationSpec spec) {
        MigrationSpec.Options opts = spec.options();
        VerifySpec v = opts.verification();
        if (verify && !v.enabled()) {
            v = new VerifySpec(true, v.sample(), v.fullHashCompare());
        }
        String env = environment != null ? environment : opts.environment();
        boolean envUnchanged = (env == null && opts.environment() == null)
                || (env != null && env.equals(opts.environment()));
        if (v == opts.verification() && envUnchanged) {
            return spec;   // nothing to override
        }
        MigrationSpec.Options newOpts = new MigrationSpec.Options(
                opts.executionMode(),
                opts.conflict(),
                opts.transform(),
                opts.performance(),
                v,
                opts.errorPolicy() == null ? ErrorPolicy.defaults() : opts.errorPolicy(),
                opts.ignoreDrift(),
                env,
                opts.sinks());
        return new MigrationSpec(
                spec.schema(), spec.kind(), spec.name(),
                spec.source(), spec.target(), spec.scope(), spec.scriptsFolder(),
                newOpts);
    }

    private void writeEvent(ObjectMapper json, JobEvent e) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("ts", e.timestamp() == null ? Instant.now() : e.timestamp());
        envelope.put("jobId", e.jobId().value());
        envelope.put("type", e.getClass().getSimpleName());
        switch (e) {
            case JobEvent.Started s ->
                    envelope.put("specName", s.spec().name());
            case JobEvent.StatusChanged sc ->
                    envelope.put("newStatus", sc.newStatus().name());
            case JobEvent.Progress p -> {
                ProgressSnapshot snap = p.snapshot();
                Map<String, Object> prog = new LinkedHashMap<>();
                prog.put("docsCopied", snap.docsCopied());
                if (snap.docsTotal() >= 0) prog.put("docsTotal", snap.docsTotal());
                prog.put("bytesCopied", snap.bytesCopied());
                prog.put("docsPerSec", snap.docsPerSecRolling());
                prog.put("mbPerSec", snap.mbPerSecRolling());
                prog.put("elapsed", snap.elapsed().toString());
                if (!snap.eta().isZero()) prog.put("eta", snap.eta().toString());
                prog.put("errors", snap.errors());
                envelope.put("progress", prog);
                envelope.put("collections", snap.perCollection().size());
                if (!snap.perCollection().isEmpty()) {
                    envelope.put("lastCollection", summarise(snap.perCollection().get(0)));
                }
            }
            case JobEvent.LogLine l -> {
                envelope.put("level", l.level());
                envelope.put("collection", l.collection());
                envelope.put("op", l.op());
                envelope.put("message", l.message());
            }
            case JobEvent.Completed c ->
                    envelope.put("verification", c.report());
            case JobEvent.Failed f -> {
                envelope.put("error", f.error());
                if (f.resumeFile() != null) envelope.put("resumeFile", f.resumeFile().toString());
            }
            case JobEvent.Cancelled x -> {
                if (x.resumeFile() != null) envelope.put("resumeFile", x.resumeFile().toString());
            }
        }
        try {
            out.println(json.writeValueAsString(envelope));
        } catch (Exception ex) {
            // Never let serialization kill the pipeline; warn and keep going.
            err.println("warn: could not serialise event " + envelope.get("type") + ": " + ex.getMessage());
        }
    }

    private static Map<String, Object> summarise(CollectionProgress cp) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("source", cp.source());
        out.put("target", cp.target());
        out.put("docsCopied", cp.docsCopied());
        if (cp.docsTotal() >= 0) out.put("docsTotal", cp.docsTotal());
        out.put("status", cp.status());
        return out;
    }

    private static boolean await(CountDownLatch latch) throws InterruptedException {
        latch.await();
        return true;
    }

    private static int exitFor(JobStatus status) {
        if (status == null) return 65;
        return switch (status) {
            case COMPLETED, COMPLETED_WITH_WARNINGS -> 0;
            case FAILED -> 1;
            case CANCELLED -> 2;
            default -> 65;   // shouldn't happen — only terminal states reach us
        };
    }

    public static void main(String[] args) {
        int code = new CommandLine(new MigrateCli()).execute(args);
        // picocli returns its own code for argument errors; pass it through if non-zero and
        // our run() hasn't already called System.exit via a successful parse.
        if (code != 0) System.exit(code);
    }
}
