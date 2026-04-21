package com.kubrik.mex.backup.runner;

import com.kubrik.mex.backup.spec.BackupPolicy;
import com.kubrik.mex.backup.store.BackupCatalogDao;
import com.kubrik.mex.backup.store.BackupCatalogRow;
import com.kubrik.mex.backup.store.BackupPolicyDao;
import com.kubrik.mex.backup.store.BackupStatus;
import com.kubrik.mex.migration.schedule.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * v2.5 BKP-SCHED-1..5 — ticking scheduler for backup policies.
 *
 * <p>Cadence: wakes every 60 seconds (aligned with the cron granularity). On
 * each tick walks every enabled policy, parses its {@code scheduleCron}, and
 * if the expression matches the current UTC minute invokes
 * {@code dispatcher.accept(policy)}.</p>
 *
 * <p>Concurrency (BKP-SCHED-4): tracks in-flight policy ids in
 * {@link #running}; a tick that would re-trigger an already-running policy is
 * skipped, leaving the next cron tick to fire it. Callers must invoke
 * {@link #markDone} after the run finishes (success or fail) so the policy
 * re-enters the eligible set.</p>
 *
 * <p>Orphan reconciliation (BKP-SCHED-2): on {@link #start()} every catalog
 * row with {@code status = RUNNING} is flipped to {@code FAILED} with a
 * "process crashed mid-run" note. Missed-runs backfill walks the cron
 * backwards {@link #MISSED_WINDOW} from {@code now} — for each firing that
 * predates the policy's most-recent catalog row, a synthetic
 * {@link BackupStatus#MISSED} row is written.</p>
 */
public final class BackupScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BackupScheduler.class);

    static final Duration MISSED_WINDOW = Duration.ofHours(24);
    static final Duration TICK_INTERVAL = Duration.ofSeconds(60);

    private final BackupPolicyDao policies;
    private final BackupCatalogDao catalog;
    private final Consumer<BackupPolicy> dispatcher;
    private final Clock clock;
    private final ZoneId zone;
    private final ScheduledExecutorService exec;

    private final ConcurrentMap<Long, Instant> lastFiredByPolicy = new ConcurrentHashMap<>();
    private final Set<Long> running = ConcurrentHashMap.newKeySet();
    private ScheduledFuture<?> scheduled;
    private volatile boolean closed = false;

    public BackupScheduler(BackupPolicyDao policies, BackupCatalogDao catalog,
                           Consumer<BackupPolicy> dispatcher, Clock clock) {
        this.policies = Objects.requireNonNull(policies);
        this.catalog = Objects.requireNonNull(catalog);
        this.dispatcher = Objects.requireNonNull(dispatcher);
        this.clock = clock == null ? Clock.systemUTC() : clock;
        this.zone = ZoneId.of("UTC");
        this.exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "backup-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        reconcileOrphans();
        backfillMissed();
        scheduled = exec.scheduleAtFixedRate(this::tick,
                TICK_INTERVAL.toMillis(), TICK_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        closed = true;
        if (scheduled != null) scheduled.cancel(false);
        exec.shutdownNow();
    }

    /** Caller invokes after a dispatched run terminates (any status) so the
     *  policy re-enters the eligible set. Safe to call repeatedly. */
    public void markDone(long policyId) { running.remove(policyId); }

    /* ============================== ticks =============================== */

    /** Visible for tests: runs one tick synchronously. Safe to call while
     *  {@link #start()} is also scheduling ticks — the dedup map prevents
     *  the same minute from firing twice. */
    public void tick() {
        if (closed) return;
        Instant now = clock.instant();
        for (BackupPolicy p : policies.listEnabled()) {
            if (p.scheduleCron() == null || p.scheduleCron().isBlank()) continue;
            CronExpression cron;
            try { cron = CronExpression.parse(p.scheduleCron()); }
            catch (IllegalArgumentException bad) { continue; }
            if (!cron.matches(now, zone)) continue;

            // One firing per minute — guard against multiple ticks in the same
            // minute if TICK_INTERVAL drifts.
            Instant thisMinute = now.atZone(zone).withSecond(0).withNano(0).toInstant();
            Instant last = lastFiredByPolicy.get(p.id());
            if (last != null && !thisMinute.isAfter(last)) continue;

            if (!running.add(p.id())) {
                log.info("scheduler: policy {} already running, skipping this minute",
                        p.id());
                continue;
            }
            lastFiredByPolicy.put(p.id(), thisMinute);
            try {
                dispatcher.accept(p);
            } catch (Exception e) {
                log.warn("scheduler dispatch failed for policy {}", p.id(), e);
                running.remove(p.id());
            }
        }
    }

    /* ============================ reconcile ============================ */

    /** BKP-SCHED-2: catalog rows left in RUNNING by a JVM crash can't be
     *  resumed; flip them to FAILED with a note so the history surface is
     *  accurate on startup. */
    public void reconcileOrphans() {
        long now = clock.millis();
        // listEnabled is fine here — the scheduler only owns this kind of
        // cleanup for policies it's responsible for. Disabled policies keep
        // their RUNNING row until the user re-enables or deletes them.
        for (BackupPolicy p : policies.listEnabled()) {
            for (BackupCatalogRow row : catalog.listForPolicy(p.id())) {
                if (row.status() != BackupStatus.RUNNING) continue;
                catalog.finalise(row.id(), BackupStatus.FAILED, now,
                        null, null, null, null, null,
                        "process crashed mid-run — reconciled on scheduler start");
                log.info("scheduler: reconciled orphan catalog row {} for policy {}",
                        row.id(), p.id());
            }
        }
    }

    /** BKP-SCHED-2: writes one {@link BackupStatus#MISSED} catalog row for
     *  every cron firing inside {@link #MISSED_WINDOW} that doesn't already
     *  have a catalog entry (by started_at timestamp). Bounded by the window
     *  so an absent-for-a-year policy doesn't flood the catalog. */
    public void backfillMissed() {
        Instant now = clock.instant();
        Instant floor = now.minus(MISSED_WINDOW);
        for (BackupPolicy p : policies.listEnabled()) {
            if (p.scheduleCron() == null || p.scheduleCron().isBlank()) continue;
            CronExpression cron;
            try { cron = CronExpression.parse(p.scheduleCron()); }
            catch (IllegalArgumentException bad) { continue; }

            // Collect every firing in [floor, now) by walking nextFireAfter.
            List<BackupCatalogRow> existing = catalog.listForPolicy(p.id());
            Set<Long> existingStartMinuteMs = new HashSet<>();
            for (BackupCatalogRow row : existing) {
                existingStartMinuteMs.add(row.startedAt() / 60_000 * 60_000);
            }
            Instant cursor = cron.nextFireAfter(floor.minusSeconds(60), zone);
            while (cursor.isBefore(now)) {
                long minuteMs = cursor.toEpochMilli() / 60_000 * 60_000;
                if (!existingStartMinuteMs.contains(minuteMs)) {
                    BackupCatalogRow missed = new BackupCatalogRow(
                            -1L, p.id(), p.connectionId(),
                            cursor.toEpochMilli(), cursor.toEpochMilli(),
                            BackupStatus.MISSED, p.sinkId(),
                            "<missed>", null, null, null, null, null, null, null,
                            "missed at " + cursor + " (app was closed)");
                    try { catalog.insert(missed); }
                    catch (Exception e) {
                        log.debug("missed backfill insert failed: {}", e.getMessage());
                    }
                }
                cursor = cron.nextFireAfter(cursor, zone);
            }
        }
    }
}
