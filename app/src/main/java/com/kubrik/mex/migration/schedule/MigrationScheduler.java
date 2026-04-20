package com.kubrik.mex.migration.schedule;

import com.kubrik.mex.migration.MigrationService;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.migration.store.ProfileStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** UX-7 — single-thread background loop that fires due schedules. One virtual thread per
 *  app instance wakes on a fixed cadence (default 30s), calls
 *  {@link ScheduleStore#listDue(Instant)}, dispatches each row through a user-supplied
 *  {@link Consumer} (normally wired to {@link MigrationService#start(MigrationSpec)}), then
 *  advances {@code next_run_at} via {@link CronExpression#nextFireAfter}.
 *  <p>
 *  Lifecycle matches the app: {@link #start()} on boot, {@link #close()} on quit. Schedules
 *  are local-only — the runtime has no cross-instance coordination ("the app must be running"
 *  caveat from §7.2 of the v2.0 milestone). */
public final class MigrationScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MigrationScheduler.class);

    private final ScheduleStore schedules;
    private final ProfileStore profiles;
    private final Consumer<MigrationSpec> dispatcher;
    private final Clock clock;
    private final ZoneId zone;
    private final Duration tickInterval;

    private volatile Thread loop;
    private volatile boolean stopping;

    public MigrationScheduler(ScheduleStore schedules,
                              ProfileStore profiles,
                              Consumer<MigrationSpec> dispatcher) {
        this(schedules, profiles, dispatcher, Clock.systemDefaultZone(),
                ZoneId.systemDefault(), Duration.ofSeconds(30));
    }

    /** Test-visible ctor — accepts a custom {@link Clock} / cadence for deterministic suites. */
    public MigrationScheduler(ScheduleStore schedules,
                              ProfileStore profiles,
                              Consumer<MigrationSpec> dispatcher,
                              Clock clock,
                              ZoneId zone,
                              Duration tickInterval) {
        this.schedules = schedules;
        this.profiles = profiles;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.zone = zone;
        this.tickInterval = tickInterval;
    }

    /** Starts the background loop. No-op if already started. */
    public synchronized void start() {
        if (loop != null) return;
        stopping = false;
        loop = Thread.ofVirtual()
                .name("mex-migration-scheduler")
                .start(this::runLoop);
        log.info("migration scheduler started — tick every {}s", tickInterval.toSeconds());
    }

    /** Stops the loop and waits briefly for it to exit cleanly. */
    @Override
    public synchronized void close() {
        stopping = true;
        Thread t = loop;
        if (t != null) {
            t.interrupt();
            try { t.join(TimeUnit.SECONDS.toMillis(2)); }
            catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }
        loop = null;
        log.info("migration scheduler stopped");
    }

    /** Run one sweep of the schedule table. Exposed for tests so the loop's work can be
     *  exercised on demand without racing the background thread. */
    public void tick() {
        Instant now = clock.instant();
        for (Schedule schedule : schedules.listDue(now)) {
            fire(schedule, now);
        }
    }

    private void runLoop() {
        while (!stopping && !Thread.currentThread().isInterrupted()) {
            try {
                tick();
            } catch (Throwable t) {
                log.warn("scheduler tick failed: {}", t.getMessage(), t);
            }
            try { Thread.sleep(tickInterval.toMillis()); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
        }
    }

    private void fire(Schedule schedule, Instant now) {
        MigrationSpec spec;
        try {
            spec = profiles.get(schedule.profileId())
                    .orElseThrow(() -> new IllegalStateException(
                            "schedule " + schedule.id() + " references missing profile " + schedule.profileId()))
                    .spec();
        } catch (Exception e) {
            log.warn("schedule {} skipped — {}", schedule.id(), e.getMessage());
            advanceWithoutFiring(schedule, now);
            return;
        }

        try {
            dispatcher.accept(spec);
            log.info("schedule {} fired profile `{}`", schedule.id(), schedule.profileId());
            Instant next = computeNextRun(schedule, now);
            schedules.recordRun(schedule.id(), now, next);
        } catch (Exception e) {
            // Dispatcher may refuse (concurrency cap, service stopped, etc). Move next_run_at
            // forward anyway so we don't spin-fire every tick — the user can catch up on the
            // next scheduled window.
            log.warn("schedule {} dispatch failed: {}", schedule.id(), e.getMessage());
            advanceWithoutFiring(schedule, now);
        }
    }

    private void advanceWithoutFiring(Schedule schedule, Instant now) {
        try {
            Instant next = computeNextRun(schedule, now);
            schedules.recordRun(schedule.id(), schedule.lastRunAt(), next);
        } catch (Exception e) {
            // Can't parse cron — disable the schedule so we stop hammering the row.
            log.warn("disabling schedule {} — cron invalid: {}", schedule.id(), e.getMessage());
            schedules.setEnabled(schedule.id(), false);
        }
    }

    private Instant computeNextRun(Schedule schedule, Instant now) {
        return CronExpression.parse(schedule.cron()).nextFireAfter(now, zone);
    }
}
