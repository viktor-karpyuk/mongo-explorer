package com.kubrik.mex.migration;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.migration.engine.BsonJson;
import com.kubrik.mex.migration.engine.JobContext;
import com.kubrik.mex.migration.engine.JobRunner;
import com.kubrik.mex.migration.events.JobStatus;
import com.kubrik.mex.migration.gate.PreconditionGate;
import com.kubrik.mex.migration.preflight.PreflightChecker;
import com.kubrik.mex.migration.preflight.PreflightReport;
import com.kubrik.mex.migration.profile.ProfileCodec;
import com.kubrik.mex.migration.resume.ResumeFile;
import com.kubrik.mex.migration.resume.ResumeManager;
import com.kubrik.mex.migration.resume.ResumeState;
import com.kubrik.mex.migration.spec.ExecutionMode;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.migration.store.MigrationJobDao;
import com.kubrik.mex.migration.store.ProfileStore;
import com.kubrik.mex.store.AppPaths;
import com.kubrik.mex.store.ConnectionStore;
import com.kubrik.mex.store.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Entry point for the migration feature (see docs/mvp-technical-spec.md §3.1).
 *  <p>
 *  M-1 scope delivered:
 *  <ul>
 *    <li>{@code start}, {@code startDryRun}, {@code pause}, {@code cancel}, {@code resume}</li>
 *    <li>SQLite-backed history via {@link MigrationJobDao}</li>
 *    <li>Preflight + JobRunner orchestration with virtual-thread pipelines</li>
 *  </ul>
 *  Follow-up milestones add profiles, verification, versioned migrations, and the UI shell.
 */
public final class MigrationService {

    private static final Logger log = LoggerFactory.getLogger(MigrationService.class);
    private static final int MAX_CONCURRENT_JOBS = 2;        // BR-11

    private final ConnectionManager manager;
    private final ConnectionStore store;
    private final Database db;
    private final EventBus bus;
    private final PreconditionGate preconditions;

    private final ProfileCodec codec = new ProfileCodec();
    private final MigrationJobDao jobs;
    private final ProfileStore profiles;
    private final com.kubrik.mex.migration.schedule.ScheduleStore schedules;
    private final PreflightChecker preflightChecker;
    private final ScheduledExecutorService scheduler;
    private final ConcurrentHashMap<JobId, JobRunner> active = new ConcurrentHashMap<>();
    private final AtomicLong activeCount = new AtomicLong();

    public MigrationService(ConnectionManager manager,
                            ConnectionStore store,
                            Database db,
                            EventBus bus,
                            PreconditionGate preconditions) {
        this.manager = manager;
        this.store = store;
        this.db = db;
        this.bus = bus;
        this.preconditions = preconditions;
        this.jobs = new MigrationJobDao(db, codec);
        this.profiles = new ProfileStore(db, codec);
        this.schedules = new com.kubrik.mex.migration.schedule.ScheduleStore(db);
        this.preflightChecker = new PreflightChecker(manager);
        this.scheduler = Executors.newScheduledThreadPool(1, daemonFactory("migration-scheduler"));

        int reconciled = jobs.reconcileOrphans(
                ProcessHandle.current().pid(),
                "Process terminated unexpectedly");
        if (reconciled > 0) {
            log.warn("Reconciled {} orphaned migration job(s) from a prior process", reconciled);
        }
        log.info("MigrationService ready (M-1 engine core)");
    }

    // --- Preflight ---------------------------------------------------------------------------

    public PreflightReport preflight(MigrationSpec spec) {
        preconditions.check(spec);
        return preflightChecker.check(spec);
    }

    // --- Job lifecycle -----------------------------------------------------------------------

    public JobId start(MigrationSpec spec) { return launch(spec, false); }

    public JobId startDryRun(MigrationSpec spec) { return launch(spec, true); }

    private JobId launch(MigrationSpec userSpec, boolean forceDryRun) {
        preconditions.check(userSpec);

        if (activeCount.get() >= MAX_CONCURRENT_JOBS) {
            throw new IllegalStateException(
                    "Two migrations are already running. Wait for one to finish or cancel it before starting another.");
        }

        MigrationSpec spec = forceDryRun ? withExecutionMode(userSpec, ExecutionMode.DRY_RUN) : userSpec;
        PreflightReport report = preflightChecker.check(spec);
        if (report.hasBlockingErrors()) {
            throw new IllegalStateException("Preflight errors: " + String.join("; ", report.errors()));
        }

        JobId jobId = JobId.generate();
        Path jobDir = AppPaths.migrationJobDir(jobId.value());
        try {
            Files.createDirectories(jobDir);
        } catch (Exception e) {
            throw new RuntimeException("cannot create job dir: " + jobDir, e);
        }
        ResumeManager resume = new ResumeManager(jobDir);
        String specHash = codec.specHash(spec);

        jobs.insert(JobRunner.initialRecord(jobId, spec, specHash, jobDir, resume,
                resolveConnectionName(spec.source().connectionId()),
                resolveConnectionName(spec.target().connectionId())));
        startRunner(jobId, spec, report, jobDir, specHash, ResumeState.fresh());
        return jobId;
    }

    /** Look up the user-facing name for a connection id so it can be persisted onto the
     *  job row at insert time (UX-11). Returns {@code null} if the id is not found —
     *  {@code MigrationJobDao} stores null and the UI falls back to the raw id. */
    private String resolveConnectionName(String connectionId) {
        if (connectionId == null) return null;
        try {
            var c = store.get(connectionId);
            return c == null ? null : c.name();
        } catch (Exception e) {
            return null;
        }
    }

    /** Resume a previously-interrupted job. The spec hash must match the one stored with the
     *  job — if the caller edited the spec since, this throws. On success a new run is kicked
     *  off against the same {@code JobId}, continuing from the last checkpoint. */
    public JobId resume(JobId jobId) {
        if (activeCount.get() >= MAX_CONCURRENT_JOBS) {
            throw new IllegalStateException("Two migrations are already running.");
        }
        MigrationJobRecord record = jobs.get(jobId)
                .orElseThrow(() -> new IllegalStateException("Unknown job: " + jobId));
        if (record.status().isActive()) {
            throw new IllegalStateException("Job " + jobId + " is still running.");
        }
        MigrationSpec spec = record.spec();
        preconditions.check(spec);

        Path jobDir = record.artifactDir() != null ? record.artifactDir()
                : AppPaths.migrationJobDir(jobId.value());
        ResumeManager resume = new ResumeManager(jobDir);
        ResumeFile file = resume.load().orElse(null);

        ResumeState state;
        if (file == null) {
            state = ResumeState.fresh();
        } else {
            if (!file.specHash().equals(record.specHash())) {
                throw new IllegalStateException(
                        "This job's spec has changed since it was interrupted. Start a new migration instead.");
            }
            Set<String> completed = new HashSet<>(file.completed());
            String inProgressNs = file.inProgress() != null ? file.inProgress().collection() : null;
            var inProgressLastId = file.inProgress() != null
                    ? BsonJson.fromJson(file.inProgress().lastIdJson()) : null;
            state = new ResumeState(completed, inProgressNs, inProgressLastId);
        }

        PreflightReport report = preflightChecker.check(spec);
        if (report.hasBlockingErrors()) {
            throw new IllegalStateException("Preflight errors on resume: " + String.join("; ", report.errors()));
        }

        startRunner(jobId, spec, report, jobDir, record.specHash(), state);
        return jobId;
    }

    private void startRunner(JobId jobId,
                             MigrationSpec spec,
                             PreflightReport report,
                             Path jobDir,
                             String specHash,
                             ResumeState resumeState) {
        JobRunner runner = new JobRunner(
                jobId, spec, manager, report, bus, jobs, scheduler, jobDir, specHash, resumeState);
        active.put(jobId, runner);
        activeCount.incrementAndGet();
        jobs.stampOwnership(jobId, ProcessHandle.current().pid(), java.time.Instant.now());
        Thread.ofVirtual().name("migration-job-" + jobId.value()).start(() -> {
            try {
                runner.run();
            } finally {
                active.remove(jobId);
                activeCount.decrementAndGet();
            }
        });
    }

    public void pause(JobId jobId) {
        JobRunner r = active.get(jobId);
        if (r == null) throw new IllegalStateException("No such active job: " + jobId);
        r.context().pause();
        jobs.updateStatus(jobId, JobStatus.PAUSED, null);
    }

    /** Unpause an in-memory job that was previously {@link #pause}d. The reader/writer
     *  workers wake up and continue streaming — no new job is created. */
    public void resumeLive(JobId jobId) {
        JobRunner r = active.get(jobId);
        if (r == null) throw new IllegalStateException("No such active job: " + jobId);
        r.context().resumeRun();
        jobs.updateStatus(jobId, JobStatus.RUNNING, null);
    }

    public void cancel(JobId jobId) {
        JobRunner r = active.get(jobId);
        if (r == null) throw new IllegalStateException("No such active job: " + jobId);
        r.context().stop("cancelled by user");
        jobs.updateStatus(jobId, JobStatus.CANCELLING, null);
    }

    // --- Queries -----------------------------------------------------------------------------

    public Optional<MigrationJobRecord> get(JobId jobId) { return jobs.get(jobId); }

    public List<MigrationJobRecord> list(JobHistoryQuery query) { return jobs.list(query); }

    /** Total count matching {@code query}'s filters — offset + limit ignored. Used by the
     *  history pager to compute page-of-N. */
    public long count(JobHistoryQuery query) { return jobs.count(query); }

    /** Access to the profile DAO. */
    public ProfileStore profiles() { return profiles; }

    public com.kubrik.mex.migration.schedule.ScheduleStore schedules() { return schedules; }

    /** Roll back versioned migrations on a target database to (and not including) the given
     *  version. Scans the scripts folder for {@code U*} rollback files and executes them
     *  newest-first. Rows in {@code _mongo_explorer_migrations} are preserved and marked
     *  {@code ROLLED_BACK} — the audit trail is intact (T-10). */
    public com.kubrik.mex.migration.versioned.Rollback.Result rollbackVersioned(
            String targetConnectionId,
            String targetDatabase,
            String scriptsFolder,
            String toVersion) {
        var svc = manager.service(targetConnectionId);
        if (svc == null) throw new IllegalStateException("Target connection not active.");
        return new com.kubrik.mex.migration.versioned.Rollback().rollback(
                new com.kubrik.mex.migration.versioned.Rollback.Request(
                        svc.database(targetDatabase),
                        java.nio.file.Path.of(scriptsFolder),
                        toVersion,
                        System.getProperty("user.name", "mongo-explorer")));
    }

    /** Access to the codec (so the UI can build diffs / previews). */
    public ProfileCodec codec() { return codec; }

    public List<MigrationJobRecord> unfinishedOnStartup() {
        return jobs.list(JobHistoryQuery.all()).stream()
                .filter(r -> !r.status().isTerminal())
                .toList();
    }

    // --- helpers -----------------------------------------------------------------------------

    private static MigrationSpec withExecutionMode(MigrationSpec spec, ExecutionMode mode) {
        MigrationSpec.Options opts = spec.options();
        MigrationSpec.Options newOpts = new MigrationSpec.Options(
                mode, opts.conflict(), opts.transform(), opts.performance(),
                opts.verification(), opts.errorPolicy(), opts.ignoreDrift(), opts.environment(),
                opts.sinks());
        return new MigrationSpec(spec.schema(), spec.kind(), spec.name(),
                spec.source(), spec.target(), spec.scope(), spec.scriptsFolder(), newOpts);
    }

    private static ThreadFactory daemonFactory(String name) {
        AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, name + "-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }
}
