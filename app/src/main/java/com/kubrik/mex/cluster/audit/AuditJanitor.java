package com.kubrik.mex.cluster.audit;

import com.kubrik.mex.store.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * v2.4 AUD-RET-1..2 — periodic purge of {@code ops_audit}.
 *
 * <p>Default cadence: once per day at {@link #RUN_AT} local time. Rows older
 * than {@link #retentionDays} are eligible for deletion <em>unless</em> they
 * carry {@code outcome = FAIL} or a {@code role_used} in
 * {@link #EXEMPT_ROLES} — those are preserved indefinitely until explicitly
 * cleared by the user. The exemption mirrors the spec's "never auto-purge
 * failures / cluster-admin rows" guarantee in AUD-RET-2.</p>
 */
public final class AuditJanitor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AuditJanitor.class);

    /** Local time of day the daily sweep runs. 03:00 matches the spec §2.6. */
    public static final LocalTime RUN_AT = LocalTime.of(3, 0);

    /** Roles whose audit rows are exempt from auto-purge. */
    public static final java.util.List<String> EXEMPT_ROLES = java.util.List.of("root", "clusterAdmin");

    private final Database db;
    private final Clock clock;
    private final int retentionDays;
    private final ScheduledExecutorService exec;
    private ScheduledFuture<?> scheduled;
    private volatile boolean closed = false;

    public AuditJanitor(Database db, Clock clock, int retentionDays) {
        this.db = db;
        this.clock = clock == null ? Clock.systemDefaultZone() : clock;
        this.retentionDays = Math.max(1, retentionDays);
        this.exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "audit-janitor");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        scheduleNext();
    }

    @Override
    public void close() {
        closed = true;
        if (scheduled != null) scheduled.cancel(false);
        exec.shutdownNow();
    }

    /** Runs one sweep now, returning the number of rows deleted. Visible for tests. */
    public int sweep() {
        long cutoffMs = clock.instant().minus(retentionDays, ChronoUnit.DAYS).toEpochMilli();
        String sql = "DELETE FROM ops_audit WHERE started_at < ? "
                + "AND outcome <> 'FAIL' "
                + "AND (role_used IS NULL OR role_used NOT IN ('root','clusterAdmin'))";
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setLong(1, cutoffMs);
            int n = ps.executeUpdate();
            log.info("ops_audit janitor: purged {} rows older than {} days", n, retentionDays);
            return n;
        } catch (SQLException e) {
            log.warn("ops_audit janitor failed", e);
            return 0;
        }
    }

    private void scheduleNext() {
        if (closed) return;
        long delayMs = delayUntilNextRun();
        scheduled = exec.schedule(() -> {
            try { sweep(); } catch (Exception e) { log.warn("janitor sweep crashed", e); }
            scheduleNext();
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private long delayUntilNextRun() {
        ZoneId zone = clock.getZone();
        ZonedDateTime now = ZonedDateTime.now(clock);
        ZonedDateTime next = ZonedDateTime.of(LocalDateTime.of(now.toLocalDate(), RUN_AT), zone);
        if (!next.isAfter(now)) {
            next = ZonedDateTime.of(LocalDateTime.of(LocalDate.now(clock).plusDays(1), RUN_AT), zone);
        }
        return Math.max(1_000L, next.toInstant().toEpochMilli() - now.toInstant().toEpochMilli());
    }
}
