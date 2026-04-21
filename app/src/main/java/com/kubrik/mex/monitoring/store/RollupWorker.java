package com.kubrik.mex.monitoring.store;

import com.kubrik.mex.monitoring.model.RollupTier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Wakes every {@code windowMs / 4} per tier, rolls the previous aligned window
 * from the finer tier into this one. See technical-spec §5.3.
 */
public final class RollupWorker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RollupWorker.class);

    private final RollupDao dao;
    private final Clock clock;
    private final ScheduledExecutorService exec =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "mex-mon-rollup");
                t.setDaemon(true);
                return t;
            });

    private final ScheduledFuture<?>[] futures = new ScheduledFuture<?>[3];

    /** Remembers the last window boundary rolled per tier — so we never duplicate work. */
    private final long[] lastWindowEnd = new long[RollupTier.values().length];

    public RollupWorker(RollupDao dao, Clock clock) {
        this.dao = dao;
        this.clock = clock;
        for (int i = 0; i < lastWindowEnd.length; i++) lastWindowEnd[i] = -1;
    }

    public RollupWorker(RollupDao dao) { this(dao, Clock.systemUTC()); }

    public void start() {
        futures[0] = schedule(RollupTier.S10);
        futures[1] = schedule(RollupTier.M1);
        futures[2] = schedule(RollupTier.H1);
    }

    /** Run one tick of all tiers synchronously (tests). */
    public void tickAll() throws SQLException {
        runOnce(RollupTier.S10);
        runOnce(RollupTier.M1);
        runOnce(RollupTier.H1);
    }

    @Override
    public void close() {
        for (var f : futures) if (f != null) f.cancel(true);
        exec.shutdownNow();
    }

    private ScheduledFuture<?> schedule(RollupTier tier) {
        long periodMs = Math.max(250L, tier.windowSize().toMillis() / 4);
        return exec.scheduleAtFixedRate(() -> {
            try { runOnce(tier); }
            catch (Throwable t) { log.warn("rollup tick failed for tier {}", tier, t); }
        }, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    public void runOnce(RollupTier tier) throws SQLException {
        long nowMs = clock.millis();
        long windowMs = tier.windowSize().toMillis();
        long alignedEnd = (nowMs / windowMs) * windowMs;
        long prev = lastWindowEnd[tier.ordinal()];
        long fromMs = prev == -1
                ? alignedEnd - windowMs * 8L
                : prev;
        if (alignedEnd <= fromMs) return;
        RollupTier source = switch (tier) {
            case RAW -> throw new IllegalArgumentException("RAW cannot be rolled up into");
            case S10 -> RollupTier.RAW;
            case M1  -> RollupTier.S10;
            case H1  -> RollupTier.M1;
        };
        List<RollupDao.Aggregate> aggs = dao.rollupFromTier(source, tier, fromMs, alignedEnd);
        dao.upsert(tier, aggs);
        lastWindowEnd[tier.ordinal()] = alignedEnd;
        log.debug("rollup {}: {} aggregates from [{}, {})", tier, aggs.size(), fromMs, alignedEnd);
    }
}
