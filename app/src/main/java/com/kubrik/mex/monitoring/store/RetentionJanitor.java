package com.kubrik.mex.monitoring.store;

import com.kubrik.mex.monitoring.MonitoringProfile;
import com.kubrik.mex.monitoring.model.RollupTier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Periodically deletes rows older than each tier's horizon. See requirements.md §9.
 * Runs off a dedicated single-thread scheduler so a slow janitor tick never starves
 * the sampler writer (different connections on the same SQLite DB serialize at the
 * file-lock level, so this is cooperative).
 */
public final class RetentionJanitor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RetentionJanitor.class);

    private final Connection conn;
    private final RollupDao rollups;
    private final Supplier<Iterable<MonitoringProfile>> profilesSupplier;
    private final Clock clock;

    private final ScheduledExecutorService exec =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "mex-mon-retention");
                t.setDaemon(true);
                return t;
            });

    private ScheduledFuture<?> future;

    public RetentionJanitor(Connection conn, RollupDao rollups,
                            Supplier<Iterable<MonitoringProfile>> profilesSupplier,
                            Clock clock) {
        this.conn = conn;
        this.rollups = rollups;
        this.profilesSupplier = profilesSupplier;
        this.clock = clock;
    }

    public RetentionJanitor(Connection conn, RollupDao rollups,
                            Supplier<Iterable<MonitoringProfile>> profilesSupplier) {
        this(conn, rollups, profilesSupplier, Clock.systemUTC());
    }

    public void start() { start(Duration.ofSeconds(60)); }

    public void start(Duration cadence) {
        future = exec.scheduleAtFixedRate(this::runSafe,
                cadence.toMillis(), cadence.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Execute one pass synchronously. Returns the total row-deletion count. */
    public int runOnce() throws SQLException {
        int total = 0;
        long nowMs = clock.millis();
        // Snapshot the profile set so a concurrent disable() doesn't change the
        // iteration target halfway through the sweep.
        List<MonitoringProfile> snapshot = new ArrayList<>();
        for (MonitoringProfile p : profilesSupplier.get()) snapshot.add(p);
        for (MonitoringProfile p : snapshot) {
            for (RollupTier t : RollupTier.values()) {
                Duration horizon = p.retention().getOrDefault(t, t.defaultHorizon());
                long cutoff = nowMs - horizon.toMillis();
                total += deleteOlderForConn(t, p.connectionId(), cutoff);
            }
        }
        if (total > 0) log.debug("retention janitor deleted {} rows", total);
        return total;
    }

    private int deleteOlderForConn(RollupTier tier, String connectionId, long cutoff) throws SQLException {
        String sql = "DELETE FROM " + tier.tableName() + " WHERE connection_id = ? AND ts < ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, connectionId);
            ps.setLong(2, cutoff);
            return ps.executeUpdate();
        }
    }

    private void runSafe() {
        try { runOnce(); }
        catch (Throwable t) { log.warn("retention janitor tick failed", t); }
    }

    @Override
    public void close() {
        if (future != null) future.cancel(true);
        exec.shutdownNow();
    }
}
