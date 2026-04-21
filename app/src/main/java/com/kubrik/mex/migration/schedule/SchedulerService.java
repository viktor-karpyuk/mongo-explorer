package com.kubrik.mex.migration.schedule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Local scheduler — fires saved migration profiles on their schedule while the app
 * is open. Single virtual-thread tick every {@link #DEFAULT_TICK}; per-tick work:
 * load every due schedule, invoke the caller-provided runner, persist
 * {@code last_run_at} + {@code next_run_at}.
 *
 * <p>UX-7 explicitly does not guarantee cross-session continuity: if the app is
 * closed, missed fires are skipped — the next tick computes {@code nextRunAt} from
 * "now", not from when the schedule was supposed to fire. This is the "app must be
 * running" caveat from the milestone.
 */
public final class SchedulerService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SchedulerService.class);

    /** One minute — interval-based expressions with finer resolution (e.g. {@code @every 30s})
     *  still work; they just tick on the next minute boundary. */
    public static final long DEFAULT_TICK_MS = 60_000L;

    private final ScheduleDao dao;
    private final Consumer<MigrationSchedule> runner;
    private final Clock clock;
    private final ScheduledExecutorService exec =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "mex-migration-scheduler");
                t.setDaemon(true);
                return t;
            });
    private ScheduledFuture<?> tickHandle;

    public SchedulerService(ScheduleDao dao,
                            Consumer<MigrationSchedule> runner,
                            Clock clock) {
        this.dao = dao;
        this.runner = runner;
        this.clock = clock;
    }

    public SchedulerService(ScheduleDao dao, Consumer<MigrationSchedule> runner) {
        this(dao, runner, Clock.systemUTC());
    }

    public void start() { start(DEFAULT_TICK_MS); }

    public void start(long tickMs) {
        tickHandle = exec.scheduleAtFixedRate(this::safeTick, tickMs, tickMs, TimeUnit.MILLISECONDS);
    }

    /** One tick, synchronous. Useful for tests and for the UI "Run now" action. */
    public int tickOnce() throws SQLException {
        long now = clock.millis();
        List<MigrationSchedule> due = dao.dueAt(now);
        for (MigrationSchedule s : due) {
            try {
                runner.accept(s);
            } catch (Throwable t) {
                // A broken runner must not stop the scheduler thread — log, continue, and
                // still advance next_run_at so a perma-failing schedule doesn't hot-loop.
                log.warn("scheduler runner threw for schedule {} ({})", s.id(), s.expression(), t);
            }
            long next;
            try {
                next = ScheduleExpression.nextAfter(s.expression(), now);
            } catch (IllegalArgumentException iae) {
                log.warn("schedule {} has invalid expression {}: {}", s.id(), s.expression(), iae.getMessage());
                // Push far enough in the future to stop retriggering; the user must fix the row.
                next = now + TimeUnit.DAYS.toMillis(3650);
            }
            dao.markRun(s.id(), now, next);
        }
        return due.size();
    }

    @Override
    public void close() {
        if (tickHandle != null) tickHandle.cancel(true);
        exec.shutdownNow();
    }

    private void safeTick() {
        try { tickOnce(); }
        catch (Throwable t) { log.warn("scheduler tick failed", t); }
    }
}
