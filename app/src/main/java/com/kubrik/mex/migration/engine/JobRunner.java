package com.kubrik.mex.migration.engine;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.migration.JobId;
import com.kubrik.mex.migration.MigrationJobRecord;
import com.kubrik.mex.migration.events.JobEvent;
import com.kubrik.mex.migration.events.JobStatus;
import com.kubrik.mex.migration.log.JobLogger;
import com.kubrik.mex.migration.preflight.PreflightReport;
import com.kubrik.mex.migration.resume.ResumeFile;
import com.kubrik.mex.migration.resume.ResumeManager;
import com.kubrik.mex.migration.resume.ResumeState;
import com.kubrik.mex.migration.spec.ExecutionMode;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.migration.store.MigrationJobDao;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Top-level orchestrator for a single job. Runs preflight-resolved plans sequentially per
 *  collection (parallelism within a collection comes from partitions inside
 *  {@link CollectionPipeline}). A scheduled ticker periodically flushes progress to
 *  {@code migration_jobs} and the resume file. */
public final class JobRunner {

    private static final Logger log = LoggerFactory.getLogger(JobRunner.class);

    private final JobId jobId;
    private final MigrationSpec spec;
    private final ConnectionManager manager;
    private final PreflightReport preflight;
    private final EventBus bus;
    private final MigrationJobDao jobs;
    private final ScheduledExecutorService scheduler;
    private final JobContext ctx;
    private final Path jobDir;
    private final String specHash;
    private final ResumeState resumeState;
    private final JobLogger jlog;

    // Mutated by the flusher thread and the run thread; guarded by `this`.
    private final Set<String> completed = new LinkedHashSet<>();

    /** OBS-6 fixed-cadence bus publish — 200 ms wall-clock, independent of batch / partition
     *  boundaries. */
    private static final long PROGRESS_TICK_MS = 200L;
    /** DAO writes are throttled to this cadence so the 200 ms bus publish doesn't hammer
     *  SQLite 5× per second. Picked to match pre-OBS-6 behaviour (≈ every 2 s). */
    private static final long DAO_FLUSH_INTERVAL_MS = 2_000L;
    private volatile long lastDaoFlushAtMs = 0L;

    /** OBS-7 pause-aware active-time accumulator. {@link #publishTick()} samples the clock and
     *  adds the delta only if the previous sample was in a RUNNING (non-paused) state — so
     *  paused windows never contribute. Resolution matches the tick cadence. */
    private long activeMillisAccum = 0L;
    private long lastActiveSampleMillis = 0L;
    private boolean lastSamplePaused = false;

    public JobRunner(JobId jobId,
                     MigrationSpec spec,
                     ConnectionManager manager,
                     PreflightReport preflight,
                     EventBus bus,
                     MigrationJobDao jobs,
                     ScheduledExecutorService scheduler,
                     Path jobDir,
                     String specHash,
                     ResumeState resumeState) {
        this.jobId = jobId;
        this.spec = spec;
        this.manager = manager;
        this.preflight = preflight;
        this.bus = bus;
        this.jobs = jobs;
        this.scheduler = scheduler;
        this.jobDir = jobDir;
        this.specHash = specHash;
        this.resumeState = resumeState == null ? ResumeState.fresh() : resumeState;
        this.ctx = new JobContext(jobId, spec, jobDir, new ResumeManager(jobDir));
        this.jlog = new JobLogger(JobRunner.class, jobId);
        synchronized (this) {
            this.completed.addAll(this.resumeState.completedCollections());
        }
    }

    public JobContext context() { return ctx; }

    public void run() {
        Instant started = Instant.now();
        lastActiveSampleMillis = System.currentTimeMillis();
        emit(new JobEvent.Started(jobId, started, spec));
        emit(new JobEvent.StatusChanged(jobId, started, JobStatus.RUNNING));
        jobs.markStarted(jobId, started);   // OBS-7: persist started_at for history & details view
        jobs.updateStatus(jobId, JobStatus.RUNNING, null);
        jlog.info("job_started",
                "kind", spec.kind().name(),
                "executionMode", spec.options().executionMode().name(),
                "source", spec.source().connectionId(),
                "target", spec.target().connectionId(),
                "plans", preflight.plans().size());

        ScheduledFuture<?> ticker = scheduler.scheduleAtFixedRate(this::publishTick,
                PROGRESS_TICK_MS, PROGRESS_TICK_MS, TimeUnit.MILLISECONDS);

        // Initial resume file so cancel/crash within the first tick still produces something.
        flushResumeFile();

        Exception fatal = null;
        try {
            MongoService src = manager.service(spec.source().connectionId());
            MongoService tgt = manager.service(spec.target().connectionId());
            if (src == null || tgt == null) {
                throw new IllegalStateException("source/target connection not active");
            }
            if (spec.kind() == com.kubrik.mex.migration.spec.MigrationKind.VERSIONED) {
                runVersioned(tgt);
                return;
            }
            for (CollectionPlan plan : preflight.plans()) {
                if (ctx.stopping()) break;
                if (plan.isView()) continue;   // SCOPE-6 — views run after data-copy.

                synchronized (this) {
                    if (completed.contains(plan.sourceNs())) {
                        log.info("job {}: skipping already-completed {}", jobId, plan.sourceNs());
                        continue;
                    }
                }

                BsonValue resumeAfterId = resumeAfterIdFor(plan.sourceNs());
                ctx.setInProgressCollection(plan.sourceNs());
                if (resumeAfterId != null) {
                    log.info("job {}: resuming {} after _id = {}", jobId, plan.sourceNs(), resumeAfterId);
                    ctx.setLastId(plan.sourceNs(), resumeAfterId);
                }

                log.info("job {}: starting collection {}", jobId, plan.sourceNs());
                jlog.info("collection_started",
                        "coll", plan.sourceNs(),
                        "target", plan.targetNs(),
                        "conflict", plan.conflictMode().name());
                long collStart = System.currentTimeMillis();
                jobs.recordCollectionStart(jobId, plan.sourceNs());
                CollectionPipeline pipeline = new CollectionPipeline(ctx, src, tgt, plan, resumeAfterId);
                pipeline.run();

                // Only mark complete if we actually drained the collection — a cancel fires the
                // reader to stop early but pipeline.run() still returns normally. Keeping the
                // collection in the in-progress set ensures resume picks up from the checkpoint.
                if (!ctx.stopping()) {
                    synchronized (this) { completed.add(plan.sourceNs()); }
                    ctx.clearInProgressCollection(plan.sourceNs());
                    jobs.recordCollectionEnd(jobId, plan.sourceNs());
                    jlog.info("collection_done",
                            "coll", plan.sourceNs(),
                            "durationMs", System.currentTimeMillis() - collStart,
                            "docs", ctx.metrics().docs(),
                            "bytes", ctx.metrics().bytes());
                }
                flushResumeFile();
            }

            // SCOPE-6 — Views stage. Runs after every data-copy plan has finished so any base
            // collection referenced by a view's pipeline already exists on the target. The
            // rename map must use collection-name keys (not full `db.coll`) because view
            // pipelines reference within-DB collections by bare name.
            if (!ctx.stopping()) {
                java.util.Map<String, String> localRenameMap =
                        buildLocalRenameMap(preflight.plans());
                for (CollectionPlan plan : preflight.plans()) {
                    if (ctx.stopping()) break;
                    if (!plan.isView()) continue;
                    log.info("job {}: creating view {}", jobId, plan.targetNs());
                    jlog.info("view_started",
                            "coll", plan.sourceNs(),
                            "target", plan.targetNs());
                    try {
                        new com.kubrik.mex.migration.engine.ViewCreator(
                                ctx, src, tgt, plan, localRenameMap).run();
                        jlog.info("view_done", "coll", plan.targetNs());
                    } catch (Exception e) {
                        // Views are advisory — a failed view shouldn't tank an otherwise-good
                        // data migration. Log and carry on.
                        log.warn("job {}: view {} failed: {}", jobId, plan.targetNs(), e.getMessage());
                        jlog.warn("view_failed",
                                "coll", plan.targetNs(),
                                "error", e.getMessage());
                    }
                }
            }

            // SCOPE-12 — Users stage runs after all collections for the selected DBs finish.
            // Guarded by the scope flag and skipped in dry-run / cancellation paths.
            if (!ctx.stopping()
                    && spec.scope() != null
                    && spec.scope().migrateUsers()
                    && spec.options().executionMode() != ExecutionMode.DRY_RUN) {
                copyUsers(src, tgt);
            }
        } catch (Exception e) {
            fatal = e;
            log.error("job {}: fatal error", jobId, e);
            jlog.error("job_failed", e);
        } finally {
            ticker.cancel(false);
            flushPersistence(); // final persist — unconditional, ignore throttle
            emitProgress();     // final bus tick so UI sees terminal counters pre-Completed event
            flushResumeFile(); // final resume file (may capture mid-collection state)
            finish(fatal);
        }
    }

    /** Builds a {@code sourceCollName → targetCollName} map for SCOPE-6 view rewriting.
     *  View pipelines reference within-DB collections by bare name, so we collapse the full
     *  namespace renames from the plan list into short-key form — and only for plans whose
     *  source and target databases match. Cross-DB renames can't be expressed inside a view
     *  pipeline (the server rejects a {@code $lookup.from} pointing at another DB), so
     *  silently drop them; they'll stay as their original name in the rewritten pipeline. */
    private static java.util.Map<String, String> buildLocalRenameMap(
            java.util.List<CollectionPlan> plans) {
        java.util.Map<String, String> out = new java.util.HashMap<>();
        for (CollectionPlan p : plans) {
            if (p.isView()) continue;
            Namespaces.Ns s = p.source();
            Namespaces.Ns t = p.target();
            if (!s.db().equals(t.db())) continue;
            if (s.coll().equals(t.coll())) continue;   // identity mapping — no rewrite needed
            out.put(s.coll(), t.coll());
        }
        return out;
    }

    /** SCOPE-12 Users stage. Iterates the unique set of source databases covered by the plans
     *  and copies users per DB. Failures bump {@code usersFailed} but never stop the job —
     *  {@code finish()} turns any non-zero count into {@code COMPLETED_WITH_WARNINGS}. */
    private void copyUsers(MongoService src, MongoService tgt) {
        java.util.LinkedHashSet<String> sourceDbs = new java.util.LinkedHashSet<>();
        for (CollectionPlan p : preflight.plans()) {
            int dot = p.sourceNs().indexOf('.');
            if (dot > 0) sourceDbs.add(p.sourceNs().substring(0, dot));
        }
        if (sourceDbs.isEmpty()) return;
        UsersCopier copier = new UsersCopier(src, tgt, ctx.metrics(), jlog);
        for (String db : sourceDbs) {
            if (ctx.stopping()) break;
            String targetDb = renameTargetDbFor(db, tgt);
            copier.copy(db, targetDb);
        }
    }

    /** Resolve the target database for a source database. If a {@code Rename} collapses source
     *  namespaces into a different target DB, the first matching rename wins; otherwise the
     *  user stage writes to a DB of the same name. A non-null spec target database override
     *  wins uniformly. */
    private String renameTargetDbFor(String sourceDb, MongoService tgt) {
        String explicit = spec.target().database();
        if (explicit != null && !explicit.isBlank()) return explicit;
        for (var r : spec.scope().renames()) {
            int srcDot = r.from().indexOf('.');
            int tgtDot = r.to().indexOf('.');
            if (srcDot > 0 && tgtDot > 0 && r.from().substring(0, srcDot).equals(sourceDb)) {
                return r.to().substring(0, tgtDot);
            }
        }
        return sourceDb;
    }

    private void runVersioned(MongoService tgt) {
        log.info("job {}: running versioned migration against {}", jobId, spec.target().database());
        var migrator = new com.kubrik.mex.migration.versioned.VersionedMigrator(
                ctx, tgt, spec.target().database(), spec.scriptsFolder(),
                System.getProperty("user.name", "mongo-explorer"));
        var summary = migrator.run();
        log.info("job {}: versioned summary — applied={}, skipped={}, warnings={}",
                jobId, summary.applied(), summary.skipped(), summary.warnings().size());
    }

    private BsonValue resumeAfterIdFor(String sourceNs) {
        // Only the collection listed as "in progress" in the resume file gets a resume-after-id
        // in M-1.2 — subsequent collections restart cleanly because they never began.
        if (sourceNs.equals(resumeState.inProgressCollection())) {
            return resumeState.inProgressLastId();
        }
        return null;
    }

    /** OBS-6 200 ms tick — always publishes to the bus; writes to SQLite on a throttled
     *  cadence (every {@link #DAO_FLUSH_INTERVAL_MS}). The {@link Metrics} are sampled once
     *  so each snapshot is internally coherent. */
    private void publishTick() {
        sampleActiveMillis(); // keep OBS-7 active-time fresh even if we skip the DAO write
        long now = System.currentTimeMillis();
        if (now - lastDaoFlushAtMs >= DAO_FLUSH_INTERVAL_MS) {
            lastDaoFlushAtMs = now;
            flushPersistence();
        }
        emitProgress();
    }

    /** Persist the current metrics + heartbeat. Called from {@link #publishTick()}'s throttled
     *  branch and once unconditionally at job end. */
    private void flushPersistence() {
        try {
            jobs.updateProgress(jobId,
                    ctx.metrics().docs(),
                    ctx.metrics().bytes(),
                    ctx.metrics().errors(),
                    ctx.metrics().docsProcessed(),
                    activeMillisAccum);
            jobs.heartbeat(jobId, Instant.now());
        } catch (Exception e) {
            log.warn("progress flush failed: {}", e.getMessage());
        }
    }

    /** Sample the pause-aware active-time accumulator. Called from {@link #flush()} and
     *  {@link #finish(Exception)} so the persisted {@code active_millis} is always current. */
    private synchronized long sampleActiveMillis() {
        long now = System.currentTimeMillis();
        boolean isPaused = ctx.paused();
        if (!lastSamplePaused) {
            activeMillisAccum += Math.max(0L, now - lastActiveSampleMillis);
        }
        lastActiveSampleMillis = now;
        lastSamplePaused = isPaused;
        return activeMillisAccum;
    }

    private void emitProgress() {
        try {
            long docs = ctx.metrics().docs();
            long processed = ctx.metrics().docsProcessed();
            long bytes = ctx.metrics().bytes();
            long errors = ctx.metrics().errors();
            String currentNs = ctx.inProgressCollection();
            List<com.kubrik.mex.migration.events.CollectionProgress> perColl = new java.util.ArrayList<>();
            for (CollectionPlan plan : preflight.plans()) {
                String ns = plan.sourceNs();
                String status;
                synchronized (this) {
                    if (completed.contains(ns)) status = "DONE";
                    else if (ns.equals(currentNs)) status = "RUNNING";
                    else status = "PENDING";
                }
                long rowDocs = ns.equals(currentNs) ? docs : 0L;
                long rowProcessed = ns.equals(currentNs) ? processed : 0L;
                perColl.add(new com.kubrik.mex.migration.events.CollectionProgress(
                        ns, plan.targetNs(),
                        rowDocs, rowProcessed,
                        -1L, status, 1));
            }
            var snap = new com.kubrik.mex.migration.events.ProgressSnapshot(
                    docs, processed, -1L, bytes,
                    ctx.metrics().docsPerSecRolling(),
                    ctx.metrics().mbPerSecRolling(),
                    ctx.metrics().elapsed(),
                    java.time.Duration.ZERO,
                    perColl,
                    errors);
            emit(new JobEvent.Progress(jobId, Instant.now(), snap));
        } catch (Exception ignored) {}
    }

    /** Serialises the current {completed set, in-progress collection, lastId} to resume.json. */
    private void flushResumeFile() {
        try {
            List<String> completedSnap;
            synchronized (this) { completedSnap = List.copyOf(completed); }
            String ns = ctx.inProgressCollection();
            ResumeFile.InProgress inProgress = null;
            if (ns != null) {
                BsonValue lastId = ctx.lastId(ns);
                inProgress = new ResumeFile.InProgress(
                        ns,
                        resolveTargetNs(ns),
                        BsonJson.toJson(lastId),
                        ctx.metrics().docs(),
                        List.of());
            }
            ResumeFile file = new ResumeFile(
                    1, jobId.value(), specHash, completedSnap, inProgress, Instant.now());
            ctx.resume().save(file);
        } catch (Exception e) {
            log.warn("resume file save failed: {}", e.getMessage());
        }
    }

    private String resolveTargetNs(String sourceNs) {
        for (CollectionPlan p : preflight.plans()) {
            if (p.sourceNs().equals(sourceNs)) return p.targetNs();
        }
        return sourceNs;
    }

    private void finish(Exception fatal) {
        JobStatus status;
        String error = null;
        if (fatal != null) {
            status = JobStatus.FAILED;
            error = fatal.getMessage();
        } else if (ctx.stopping()) {
            status = JobStatus.CANCELLED;
            error = ctx.cancellationReason();
        } else if ((ctx.metrics().errors() > 0 || ctx.metrics().usersFailed() > 0)
                && spec.options().executionMode() != ExecutionMode.DRY_RUN) {
            status = JobStatus.COMPLETED_WITH_WARNINGS;
        } else {
            status = JobStatus.COMPLETED;
        }

        // Run verification on successful non-dry-run data-transfer jobs (T-7).
        com.kubrik.mex.migration.verify.VerificationReport verification =
                com.kubrik.mex.migration.verify.VerificationReport.empty(jobId.value(), "1.1.0");
        // EXT-2 — when a non-Mongo sink is configured, the target Mongo
        // is intentionally empty, so the source-vs-target verifier
        // would always FAIL. Skip it; the sink's own success counters
        // are the source of truth in that mode. The Options record's
        // canonical ctor coerces a null sinks list to List.of(), so
        // one emptiness check covers both the null + empty cases.
        boolean hasFileSink = !spec.options().sinks().isEmpty();
        if (fatal == null
                && !ctx.stopping()
                && spec.options().executionMode() == ExecutionMode.RUN
                && spec.kind() == com.kubrik.mex.migration.spec.MigrationKind.DATA_TRANSFER
                && spec.options().verification().enabled()
                && hasFileSink) {
            // User explicitly enabled verification but also routed
            // to a sink — surface the skip decision instead of
            // leaving the audit drawer with an empty verification
            // report and no explanation.
            log.warn("verification skipped for job {}: file sink configured "
                    + "(spec.options.sinks not empty); Mongo target would "
                    + "always FAIL the count comparison", jobId.value());
        }
        if (fatal == null
                && !ctx.stopping()
                && spec.options().executionMode() == ExecutionMode.RUN
                && spec.kind() == com.kubrik.mex.migration.spec.MigrationKind.DATA_TRANSFER
                && spec.options().verification().enabled()
                && !hasFileSink) {
            try {
                var srcSvc = manager.service(spec.source().connectionId());
                var tgtSvc = manager.service(spec.target().connectionId());
                if (srcSvc != null && tgtSvc != null) {
                    verification = new com.kubrik.mex.migration.verify.Verifier(srcSvc, tgtSvc, spec)
                            .verify(jobId.value(), preflight.plans());
                    writeVerificationArtifacts(verification);
                    if (verification.status() == com.kubrik.mex.migration.verify.VerificationReport.Status.FAIL) {
                        status = JobStatus.FAILED;
                        error = "Verification FAIL — see verification.html";
                    } else if (verification.status() == com.kubrik.mex.migration.verify.VerificationReport.Status.WARN
                            && status == JobStatus.COMPLETED) {
                        status = JobStatus.COMPLETED_WITH_WARNINGS;
                    }
                }
            } catch (Exception e) {
                log.warn("verifier failed: {}", e.getMessage());
                if (status == JobStatus.COMPLETED) status = JobStatus.COMPLETED_WITH_WARNINGS;
            }
        }

        long finalDocs = ctx.metrics().docs();
        long finalProcessed = ctx.metrics().docsProcessed();
        long finalActive = sampleActiveMillis();
        jobs.markEnded(jobId, status, error, finalDocs, ctx.metrics().bytes(),
                finalProcessed, finalActive);
        emit(new JobEvent.StatusChanged(jobId, Instant.now(), status));
        jlog.info("job_completed",
                "status", status.name(),
                "docs", ctx.metrics().docs(),
                "bytes", ctx.metrics().bytes(),
                "errors", ctx.metrics().errors(),
                "durationMs", ctx.metrics().elapsed().toMillis());

        if (status == JobStatus.COMPLETED || status == JobStatus.COMPLETED_WITH_WARNINGS) {
            if (spec.options().executionMode() == ExecutionMode.RUN
                    && status == JobStatus.COMPLETED) {
                ctx.resume().deleteOnSuccess();
            }
            emit(new JobEvent.Completed(jobId, Instant.now(), verification));
        } else if (status == JobStatus.CANCELLED) {
            emit(new JobEvent.Cancelled(jobId, Instant.now(), ctx.resume().path()));
        } else {
            emit(new JobEvent.Failed(jobId, Instant.now(), error, ctx.resume().path()));
        }
    }

    private void writeVerificationArtifacts(com.kubrik.mex.migration.verify.VerificationReport report) {
        try {
            // JSON sibling — written alongside the HTML so external tools can consume it.
            String json = new com.fasterxml.jackson.databind.ObjectMapper()
                    .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(report);
            java.nio.file.Files.writeString(jobDir.resolve("verification.json"), json);
            com.kubrik.mex.migration.verify.HtmlReportRenderer.write(report, jobDir);
        } catch (Exception e) {
            log.warn("writing verification artifacts failed: {}", e.getMessage());
        }
    }

    private void emit(JobEvent e) {
        try { bus.publishJob(e); } catch (Exception ignored) {}
    }

    public static MigrationJobRecord initialRecord(JobId id,
                                                   MigrationSpec spec,
                                                   String specHash,
                                                   Path jobDir,
                                                   ResumeManager resume) {
        return initialRecord(id, spec, specHash, jobDir, resume, null, null);
    }

    /** Variant that captures {@code sourceConnectionName} and {@code targetConnectionName}
     *  (UX-11) so the history row stays readable if the connection is later renamed or
     *  deleted. Callers that already resolve the live name should use this overload. */
    public static MigrationJobRecord initialRecord(JobId id,
                                                   MigrationSpec spec,
                                                   String specHash,
                                                   Path jobDir,
                                                   ResumeManager resume,
                                                   String sourceConnectionName,
                                                   String targetConnectionName) {
        Instant now = Instant.now();
        return new MigrationJobRecord(
                id,
                spec.kind(),
                spec.source().connectionId(),
                spec.target().connectionId(),
                spec,
                specHash,
                JobStatus.PENDING,
                spec.options().executionMode(),
                null, null,
                0L, 0L, 0L,
                null,
                resume.path(),
                jobDir,
                now, now,
                null, null,
                sourceConnectionName, targetConnectionName,
                0L, 0L);
    }
}
