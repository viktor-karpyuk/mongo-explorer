package com.kubrik.mex.backup.runner;

import com.kubrik.mex.backup.event.RestoreEvent;
import com.kubrik.mex.backup.manifest.FileHasher;
import com.kubrik.mex.backup.store.BackupCatalogDao;
import com.kubrik.mex.backup.store.BackupCatalogRow;
import com.kubrik.mex.cluster.audit.OpsAuditRecord;
import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.safety.KillSwitch;
import com.kubrik.mex.cluster.store.OpsAuditDao;
import com.kubrik.mex.events.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * v2.5 Q2.5-E — orchestrates a {@code mongorestore} against a persisted
 * backup. Two modes:
 *
 * <dl>
 *   <dt>{@link Mode#REHEARSE}</dt>
 *   <dd>Safe-by-default. Rewrites every namespace into a sandbox prefix
 *       via {@code --nsFrom / --nsTo} pairs AND sets {@code --dryRun} so
 *       mongorestore reports what it would do without writing. The kill-
 *       switch is <em>not</em> checked (Rehearse is explicitly safe); no
 *       typed confirm required. Audited as {@code restore.rehearse}.</dd>
 *   <dt>{@link Mode#EXECUTE}</dt>
 *   <dd>Production dispatch. Refuses if the kill-switch is engaged;
 *       callers are expected to have obtained an explicit typed-confirm
 *       from the user first. Audited as {@code restore.execute}.</dd>
 * </dl>
 *
 * <p>Every outcome — OK, FAIL, CANCELLED — writes an
 * {@code ops_audit} row and publishes
 * {@link RestoreEvent.Started} / {@link RestoreEvent.Ended}.</p>
 */
public final class RestoreService {

    private static final Logger log = LoggerFactory.getLogger(RestoreService.class);

    public enum Mode { REHEARSE, EXECUTE }

    private final BackupCatalogDao catalog;
    private final OpsAuditDao audit;
    private final EventBus bus;
    private final KillSwitch killSwitch;
    private final Clock clock;
    private final Path sinkRoot;
    private final String mongorestoreBinary;

    public RestoreService(BackupCatalogDao catalog, OpsAuditDao audit, EventBus bus,
                          KillSwitch killSwitch, Clock clock, Path sinkRoot,
                          String mongorestoreBinary) {
        this.catalog = catalog;
        this.audit = audit;
        this.bus = bus;
        this.killSwitch = killSwitch;
        this.clock = clock == null ? Clock.systemUTC() : clock;
        this.sinkRoot = sinkRoot;
        this.mongorestoreBinary = mongorestoreBinary == null
                ? "mongorestore" : mongorestoreBinary;
    }

    public record RestoreResult(Outcome outcome, long durationMs, long failures,
                                String message) {}

    /**
     * Runs one restore synchronously. Callers should invoke on a virtual
     * thread.
     *
     * @param catalogId       which backup to restore
     * @param targetUri       Mongo URI of the destination cluster
     * @param mode            REHEARSE or EXECUTE
     * @param rehearsePrefix  when {@link Mode#REHEARSE}, the DB-name prefix
     *                        for the sandbox target (defaults to
     *                        {@code "rehearse_"} when null / blank)
     * @param dropBeforeRestore only honoured in EXECUTE mode — Rehearse
     *                        always runs with dryRun so it never drops
     * @param oplogReplay     forward-through the oplog slice captured in
     *                        the manifest (applies to both modes)
     * @param callerUser      audit caller metadata
     * @param callerHost      audit caller metadata
     */
    public RestoreResult execute(long catalogId, String targetUri, Mode mode,
                                  String rehearsePrefix, boolean dropBeforeRestore,
                                  boolean oplogReplay,
                                  String callerUser, String callerHost) {
        BackupCatalogRow row = catalog.byId(catalogId).orElse(null);
        if (row == null) {
            return new RestoreResult(Outcome.FAIL, 0, 0, "catalog row not found");
        }
        long startedAt = clock.millis();
        String connectionId = row.connectionId();

        if (mode == Mode.EXECUTE && killSwitch.isEngaged()) {
            writeAudit(connectionId, "restore.execute", row, mode, Outcome.CANCELLED,
                    "kill_switch_engaged", callerUser, callerHost, startedAt, startedAt, true);
            publishEnded(catalogId, connectionId, false, 0, 0, "kill_switch_engaged");
            return new RestoreResult(Outcome.CANCELLED, 0, 0, "kill_switch_engaged");
        }

        String command = mode == Mode.REHEARSE ? "restore.rehearse" : "restore.execute";
        bus.publishRestore(new RestoreEvent.Started(catalogId, connectionId,
                mode.name(), startedAt));

        Path sourceDir = sinkRoot.resolve(row.sinkPath());
        Map<String, String> nsRename = mode == Mode.REHEARSE
                ? buildRehearseRename(rehearsePrefix) : Map.of();
        boolean dryRun = mode == Mode.REHEARSE;
        boolean drop = mode == Mode.EXECUTE && dropBeforeRestore;

        MongorestoreOptions opts = new MongorestoreOptions(targetUri, sourceDir,
                nsRename, drop, dryRun, /*gzip=*/false, oplogReplay, 4);

        AtomicLong docsRestored = new AtomicLong();
        AtomicLong failures = new AtomicLong();
        AtomicReference<String> lastNs = new AtomicReference<>("");

        MongorestoreRunner runner = new MongorestoreRunner(mongorestoreBinary, opts, p -> {
            docsRestored.set(p.docsProcessed());
            lastNs.set(p.namespace());
            if (p.done()) failures.addAndGet(p.failures());
            bus.publishRestore(new RestoreEvent.Progress(catalogId, connectionId,
                    docsRestored.get(), lastNs.get(), failures.get()));
        });

        DumpOutcome outcome;
        try {
            outcome = runner.run();
        } catch (IOException ioe) {
            long finishedAt = clock.millis();
            writeAudit(connectionId, command, row, mode, Outcome.FAIL,
                    "spawn_failed: " + ioe.getMessage(),
                    callerUser, callerHost, startedAt, finishedAt, false);
            publishEnded(catalogId, connectionId, false, finishedAt - startedAt,
                    0, ioe.getMessage());
            return new RestoreResult(Outcome.FAIL, finishedAt - startedAt, 0,
                    ioe.getMessage());
        }

        long finishedAt = clock.millis();
        Outcome auditOutcome;
        String message;
        if (outcome.ok() && failures.get() == 0) {
            auditOutcome = Outcome.OK;
            message = "ok in " + outcome.durationMs() + " ms";
        } else if (outcome.killed()) {
            auditOutcome = Outcome.CANCELLED;
            message = "cancelled after " + outcome.durationMs() + " ms";
        } else {
            auditOutcome = Outcome.FAIL;
            message = (failures.get() > 0
                    ? failures.get() + " failures across namespaces"
                    : "exit code " + outcome.exitCode())
                    + (outcome.stderrTail().isEmpty() ? ""
                            : "\n" + outcome.stderrTail());
        }
        writeAudit(connectionId, command, row, mode, auditOutcome, message,
                callerUser, callerHost, startedAt, finishedAt, false);
        publishEnded(catalogId, connectionId, auditOutcome == Outcome.OK,
                finishedAt - startedAt, failures.get(), message);
        return new RestoreResult(auditOutcome, finishedAt - startedAt,
                failures.get(), message);
    }

    /* ============================= internals ============================= */

    private void publishEnded(long catalogId, String connectionId, boolean ok,
                               long durationMs, long failures, String message) {
        bus.publishRestore(new RestoreEvent.Ended(catalogId, connectionId, ok,
                durationMs, failures, message));
    }

    private static Map<String, String> buildRehearseRename(String prefix) {
        String effective = (prefix == null || prefix.isBlank()) ? "rehearse_" : prefix;
        // mongorestore supports wildcard patterns — "*.*" rewrites every
        // namespace: "<db>.<coll>" → "<prefix><db>.<coll>".
        return Map.of("*.*", effective + "*.*");
    }

    private void writeAudit(String connectionId, String commandName,
                             BackupCatalogRow row, Mode mode, Outcome outcome,
                             String message, String callerUser, String callerHost,
                             long startedAt, long finishedAt, boolean killSwitchEngaged) {
        String redacted = "{\"catalogId\":" + row.id() + ",\"mode\":\""
                + mode.name() + "\",\"sinkPath\":\""
                + row.sinkPath().replace("\"", "\\\"") + "\"}";
        String hash = FileHasher.hashBytes(redacted.getBytes(StandardCharsets.UTF_8));
        OpsAuditRecord rec = new OpsAuditRecord(-1L, connectionId, null, null,
                commandName, redacted, hash, outcome, message, null,
                startedAt, finishedAt, Math.max(0, finishedAt - startedAt),
                callerHost, callerUser, "backup.restore", false, killSwitchEngaged);
        try {
            OpsAuditRecord saved = audit.insert(rec);
            bus.publishOpsAudit(saved);
        } catch (Exception e) {
            log.warn("restore audit write failed: {}", e.getMessage());
        }
    }
}
