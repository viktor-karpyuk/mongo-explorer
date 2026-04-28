package com.kubrik.mex.backup.runner;

import com.kubrik.mex.backup.event.BackupEvent;
import com.kubrik.mex.backup.manifest.BackupManifest;
import com.kubrik.mex.backup.manifest.FileHasher;
import com.kubrik.mex.backup.manifest.FileRecord;
import com.kubrik.mex.backup.manifest.OplogSlice;
import com.kubrik.mex.backup.sink.LocalFsTarget;
import com.kubrik.mex.backup.sink.StorageTarget;
import com.kubrik.mex.backup.spec.BackupPolicy;
import com.kubrik.mex.backup.store.BackupCatalogDao;
import com.kubrik.mex.backup.store.BackupCatalogRow;
import com.kubrik.mex.backup.store.BackupFileDao;
import com.kubrik.mex.backup.store.BackupFileRow;
import com.kubrik.mex.backup.store.BackupStatus;
import com.kubrik.mex.cluster.audit.OpsAuditRecord;
import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.store.OpsAuditDao;
import com.kubrik.mex.events.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * v2.5 BKP-RUN-1..8 / BKP-SCHED-1..5 — orchestrates a single backup run.
 *
 * <p>Flow:
 * <ol>
 *   <li>Insert a RUNNING catalog row + write the {@code backup.start} audit
 *       row + publish {@link BackupEvent.Started}.</li>
 *   <li>Spawn {@link MongodumpRunner} against the output directory resolved
 *       from {@link OutputDirTemplate}. Progress callbacks drive
 *       {@link BackupEvent.Progress} on the event bus.</li>
 *   <li>On subprocess exit: walk the output directory, hash every file
 *       through {@link FileHasher}, assemble a {@link BackupManifest},
 *       write {@code manifest.json} to the sink, and batch-insert the
 *       per-file rows into {@code backup_files}.</li>
 *   <li>Finalise the catalog row with status / manifest hash / roll-ups,
 *       publish {@link BackupEvent.Ended}, and write the {@code backup.end}
 *       audit row.</li>
 * </ol>
 *
 * <p>v2.5.0 ships the LocalFsTarget path — cloud sinks (S3 / GCS / Azure /
 * SFTP) land with Q2.5-H. Multi-db / multi-namespace fan-out is <em>not</em>
 * looped yet (the {@link MongodumpCommandBuilder} comment explains why);
 * callers that need N-wide scope will get the first db / ns in this
 * release.</p>
 */
public final class BackupRunner {

    private static final Logger log = LoggerFactory.getLogger(BackupRunner.class);

    private static final String MEX_VERSION = "2.5.0";
    private static final String MANIFEST_FILE = "manifest.json";

    private final BackupCatalogDao catalog;
    private final BackupFileDao files;
    private final OpsAuditDao auditDao;
    private final EventBus bus;
    private final Clock clock;
    private final String mongodumpBinary;

    public BackupRunner(BackupCatalogDao catalog, BackupFileDao files, OpsAuditDao auditDao,
                        EventBus bus, Clock clock, String mongodumpBinary) {
        this.catalog = catalog;
        this.files = files;
        this.auditDao = auditDao;
        this.bus = bus;
        this.clock = clock == null ? Clock.systemUTC() : clock;
        this.mongodumpBinary = mongodumpBinary == null ? "mongodump" : mongodumpBinary;
    }

    public record RunResult(long catalogId, BackupStatus status,
                            String manifestSha256, long totalBytes,
                            String message) {}

    /**
     * Runs one backup synchronously. Callers should invoke on a virtual
     * thread — the subprocess + hash walk are both I/O-bound.
     *
     * @param connectionId    the connection the policy points at
     * @param uri             full Mongo URI (redacted in logs via argv builder)
     * @param policy          persisted policy record; its sinkId is not
     *                        re-read here (caller hands the resolved sink in)
     * @param sink            live {@link StorageTarget}; must be
     *                        {@link LocalFsTarget} in v2.5.0
     * @param sinkId          row id of the sink for the catalog row FK
     * @param callerUser      audit caller metadata
     * @param callerHost      audit caller metadata
     */
    public RunResult execute(String connectionId, String uri, BackupPolicy policy,
                             StorageTarget sink, long sinkId,
                             String callerUser, String callerHost) throws IOException {
        if (!(sink instanceof LocalFsTarget local)) {
            throw new IllegalArgumentException("v2.5.0 only supports LocalFsTarget; "
                    + "cloud sinks land in Q2.5-H");
        }

        Instant startedAt = clock.instant();
        String relativeOutDir = OutputDirTemplate.render(
                policy.archive().outputDirTemplate(), policy.name(),
                connectionId, startedAt);
        // Use the sink's Path accessor — string-parsing canonicalRoot()
        // breaks on Windows (non-URI form) and strips to the wrong path
        // on SMB / alternate-root mounts.
        Path sinkRootPath = local.rootPath();
        Path outDir = sinkRootPath.resolve(relativeOutDir);
        Files.createDirectories(outDir);

        BackupCatalogRow running = new BackupCatalogRow(-1,
                policy.id() > 0 ? policy.id() : null,
                connectionId, startedAt.toEpochMilli(), null,
                BackupStatus.RUNNING, sinkId, relativeOutDir,
                null, null, null, null, null, null, null, null);
        BackupCatalogRow saved = catalog.insert(running);
        long catalogId = saved.id();

        writeAudit(connectionId, "backup.start", relativeOutDir, Outcome.OK,
                "started", callerUser, callerHost, startedAt.toEpochMilli(),
                startedAt.toEpochMilli());
        bus.publishBackup(new BackupEvent.Started(catalogId, connectionId,
                startedAt.toEpochMilli()));

        // Any uncaught throwable from this point on must flip the catalog
        // row out of RUNNING; otherwise the scheduler's orphan-reconcile
        // sweep only catches it on the next JVM restart. Track whether the
        // happy path already finalised so the catch doesn't double-write.
        boolean finalised = false;
        try {

        // v2.6 Q2.6-L6 — fan multi-entry scopes out into one mongodump
        // invocation per entry. WholeCluster and single-entry scopes are a
        // one-element list, so the previous single-run path is the
        // default. Each sub-scope writes into its own subdirectory of
        // outDir so their manifests don't collide; the post-run file walk
        // below is already directory-recursive and picks them all up.
        List<com.kubrik.mex.backup.spec.Scope> fanned =
                com.kubrik.mex.backup.spec.Scope.fanOut(policy.scope());

        AtomicLong docsCopied = new AtomicLong();
        AtomicReference<String> lastNs = new AtomicReference<>("");

        DumpOutcome outcome = null;
        for (int i = 0; i < fanned.size(); i++) {
            com.kubrik.mex.backup.spec.Scope sub = fanned.get(i);
            // Single-run scopes share the top-level outDir (back-compat).
            // Multi-run scopes scope into shard-0 / shard-1 style subdirs
            // named after index so mongodump's --db collision is avoided.
            Path subOut = fanned.size() == 1 ? outDir : outDir.resolve("scope-" + i);
            if (fanned.size() > 1) Files.createDirectories(subOut);
            MongodumpOptions subOpts = new MongodumpOptions(uri, subOut, sub,
                    policy.archive(), policy.includeOplog(), 4);
            MongodumpRunner mongo = new MongodumpRunner(mongodumpBinary, subOpts, progress -> {
                docsCopied.set(progress.docsProcessed());
                lastNs.set(progress.namespace());
                bus.publishBackup(new BackupEvent.Progress(catalogId, connectionId,
                        0L, docsCopied.get(), lastNs.get()));
            });
            try {
                outcome = mongo.run();
            } catch (IOException ioe) {
                finaliseFail(catalogId, connectionId, startedAt, sinkId,
                        "mongodump spawn failed: " + ioe.getMessage(),
                        callerUser, callerHost);
                finalised = true;
                throw ioe;
            }
            if (!outcome.ok()) break;  // fail-fast on first sub-scope failure
        }

        if (outcome != null && !outcome.ok()) {
            String msg = outcome.killed()
                    ? "cancelled after " + outcome.durationMs() + " ms"
                    : "exit code " + outcome.exitCode();
            String notes = msg + (outcome.stderrTail().isEmpty() ? ""
                    : "\n--- stderr ---\n" + outcome.stderrTail());
            BackupStatus terminal = outcome.killed() ? BackupStatus.CANCELLED : BackupStatus.FAILED;
            finalise(catalogId, terminal, clock.millis(), null, null, null,
                    null, null, notes, startedAt.toEpochMilli(), connectionId,
                    relativeOutDir, callerUser, callerHost);
            finalised = true;
            return new RunResult(catalogId, terminal, null, 0L, msg);
        }

        // Walk the output dir, hash every file, build manifest + file rows.
        List<FileRecord> manifestFiles = new ArrayList<>();
        List<BackupFileRow> fileRows = new ArrayList<>();
        long totalBytes = 0L;
        String oplogRel = null;
        String oplogSha = null;
        try (var stream = Files.walk(outDir)) {
            var list = stream.filter(Files::isRegularFile).toList();
            for (Path f : list) {
                String rel = outDir.relativize(f).toString();
                long bytes = Files.size(f);
                String sha = FileHasher.hashFile(f);
                manifestFiles.add(new FileRecord(rel, bytes, sha));
                fileRows.add(new BackupFileRow(-1, catalogId,
                        relativeOutDir + "/" + rel, bytes, sha,
                        guessDb(rel), guessColl(rel), guessKind(rel)));
                totalBytes += bytes;
                // v2.5 Q2.5-F — capture the oplog slice path + hash for the
                // manifest. mongodump --oplog drops either oplog.bson or
                // oplog.bson.gz at the dump root; we pick the first such
                // file we find.
                if (oplogRel == null
                        && (rel.equals("oplog.bson") || rel.equals("oplog.bson.gz"))) {
                    oplogRel = rel;
                    oplogSha = sha;
                }
            }
        }

        // v2.5 Q2.5-F — approximate oplog window from run bounds. mongodump
        // --oplog captures entries between backup start and end, so those
        // bounds are exact modulo mongodump's internal ordering. Second
        // precision is sufficient for PITR planning (PitrPlanner matches
        // {oplog_first_ts, oplog_last_ts} with inclusive bounds).
        long finishedAtMs = clock.millis();
        OplogSlice oplogSlice = null;
        if (policy.includeOplog() && oplogRel != null && oplogSha != null) {
            // Guard against NTP steps making finishedAt < startedAt — the
            // OplogSlice record rejects that, which would leak a RUNNING row.
            long firstSec = startedAt.getEpochSecond();
            long lastSec = Math.max(firstSec,
                    java.time.Instant.ofEpochMilli(finishedAtMs).getEpochSecond());
            oplogSlice = new OplogSlice(firstSec, lastSec, oplogRel, oplogSha);
        }

        BackupManifest manifest = new BackupManifest(
                MEX_VERSION, BackupManifest.MANIFEST_VERSION,
                startedAt, policy.id(), connectionId,
                policy.scope(), policy.archive(),
                manifestFiles, oplogSlice);
        String manifestJson = manifest.toCanonicalJson();
        String manifestSha = manifest.footerSha256();
        byte[] manifestBytes = manifestJson.getBytes(StandardCharsets.UTF_8);
        Path manifestPath = outDir.resolve(MANIFEST_FILE);
        Files.write(manifestPath, manifestBytes);
        // Record the manifest itself as a file row so the verifier covers it.
        // Use the UTF-8 byte length, NOT manifestJson.length() (which is the
        // Java String char count — wrong for non-ASCII content).
        fileRows.add(new BackupFileRow(-1, catalogId,
                relativeOutDir + "/" + MANIFEST_FILE, manifestBytes.length,
                manifestSha, null, null, "manifest"));

        files.insertAll(fileRows);

        Long oplogFirstTs = oplogSlice == null ? null : oplogSlice.firstTs();
        Long oplogLastTs = oplogSlice == null ? null : oplogSlice.lastTs();
        finalise(catalogId, BackupStatus.OK, finishedAtMs, manifestSha,
                totalBytes, docsCopied.get(), oplogFirstTs, oplogLastTs,
                "ok in " + outcome.durationMs() + " ms",
                startedAt.toEpochMilli(), connectionId, relativeOutDir,
                callerUser, callerHost);
        finalised = true;

        return new RunResult(catalogId, BackupStatus.OK, manifestSha, totalBytes,
                "ok");
        } finally {
            if (!finalised) {
                // Catch-all: an exception bubbled past the happy path + the
                // explicit failure branches. Flag the catalog row so the
                // history surface doesn't lie about it, then let the throwable
                // propagate.
                try {
                    finaliseFail(catalogId, connectionId, startedAt, sinkId,
                            "runner crashed before finalisation",
                            callerUser, callerHost);
                } catch (Exception suppressed) {
                    log.warn("finalise-on-crash failed for catalog {}: {}",
                            catalogId, suppressed.getMessage());
                }
            }
        }
    }

    /* ============================= internals ============================= */

    private void finaliseFail(long catalogId, String connectionId, Instant startedAt,
                              long sinkId, String note,
                              String callerUser, String callerHost) {
        finalise(catalogId, BackupStatus.FAILED, clock.millis(), null, null, null,
                null, null, note, startedAt.toEpochMilli(), connectionId,
                "", callerUser, callerHost);
    }

    private void finalise(long catalogId, BackupStatus status, long finishedAt,
                          String manifestSha256, Long totalBytes, Long docCount,
                          Long oplogFirstTs, Long oplogLastTs, String notes,
                          long startedAt, String connectionId,
                          String sinkPath,
                          String callerUser, String callerHost) {
        catalog.finalise(catalogId, status, finishedAt, manifestSha256, totalBytes,
                docCount, oplogFirstTs, oplogLastTs, notes);
        bus.publishBackup(new BackupEvent.Ended(catalogId, connectionId, status,
                manifestSha256 == null ? "" : manifestSha256,
                totalBytes == null ? 0L : totalBytes,
                docCount == null ? 0L : docCount,
                notes == null ? "" : notes));
        Outcome auditOutcome = switch (status) {
            case OK -> Outcome.OK;
            case CANCELLED -> Outcome.CANCELLED;
            case FAILED -> Outcome.FAIL;
            case RUNNING -> Outcome.PENDING;
            case MISSED -> Outcome.FAIL;  // synthetic — treated as fail in the audit row
        };
        writeAudit(connectionId, "backup.end", sinkPath, auditOutcome,
                notes, callerUser, callerHost, startedAt, finishedAt);
    }

    private void writeAudit(String connectionId, String commandName, String sinkPath,
                            Outcome outcome, String message,
                            String callerUser, String callerHost,
                            long startedAt, long finishedAt) {
        String redacted = "{\"sinkPath\":\"" + sinkPath.replace("\"", "\\\"") + "\"}";
        OpsAuditRecord row = new OpsAuditRecord(-1L, connectionId, null, null,
                commandName, redacted, emptyHash(redacted), outcome,
                message, null, startedAt, finishedAt,
                Math.max(0, finishedAt - startedAt),
                callerHost, callerUser, "backup.runner", false, false);
        try {
            OpsAuditRecord saved = auditDao.insert(row);
            bus.publishOpsAudit(saved);
        } catch (Exception e) {
            log.warn("backup audit write failed: {}", e.getMessage());
        }
    }

    /** Audit rows require a 64-char preview hash; we derive a stable digest
     *  from the redacted command body so the schema constraint is satisfied
     *  without pulling the v2.4 DryRunRenderer into the backup path. */
    private static String emptyHash(String body) {
        return FileHasher.hashBytes(body.getBytes(StandardCharsets.UTF_8));
    }

    /* ========================= path classification ======================= */

    private static String guessDb(String rel) {
        int slash = rel.indexOf('/');
        if (slash < 0) return null;
        return rel.substring(0, slash);
    }

    private static String guessColl(String rel) {
        int slash = rel.indexOf('/');
        if (slash < 0) return null;
        String tail = rel.substring(slash + 1);
        int dot = tail.indexOf('.');
        return dot < 0 ? tail : tail.substring(0, dot);
    }

    private static String guessKind(String rel) {
        if (rel.endsWith(".metadata.json") || rel.endsWith(".metadata.json.gz"))
            return "metadata";
        if (rel.endsWith(".bson") || rel.endsWith(".bson.gz")) return "bson";
        if (rel.startsWith("oplog") || rel.contains("/oplog.")) return "oplog";
        return "other";
    }
}
