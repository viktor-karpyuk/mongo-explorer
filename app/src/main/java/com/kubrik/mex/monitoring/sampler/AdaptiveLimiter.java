package com.kubrik.mex.monitoring.sampler;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-connection rate limiter for monitoring commands.
 *
 * <p>Enforces SAFE-1:
 * <ul>
 *   <li>At most one monitoring command in flight at a time.</li>
 *   <li>Minimum inter-call gap of {@link #MIN_GAP_MS} between releases of the
 *       in-flight permit.</li>
 *   <li>At most {@link #CALLS_PER_SECOND} monitoring calls per second, enforced
 *       over a rolling 1-second token-bucket.</li>
 * </ul>
 *
 * <p>{@link #tryAcquire()} returns {@code false} if any of the three rules would be
 * violated — the caller must then skip the tick (never queue, per SAFE-4).
 *
 * <p>All timing uses {@link System#nanoTime()} to stay immune to wall-clock skew.
 */
public final class AdaptiveLimiter {

    public static final long MIN_GAP_MS = 50;
    public static final int CALLS_PER_SECOND = 20;

    private final Semaphore inFlight = new Semaphore(1);
    private final ReentrantLock bucketLock = new ReentrantLock();

    private final AtomicLong lastReleaseNanos = new AtomicLong(Long.MIN_VALUE / 2);

    /** Rolling bucket: circular array of release nanos for SAFE-1 budget. */
    private final long[] bucket = new long[CALLS_PER_SECOND];
    private int bucketHead = 0;

    /** Permit for the pluggable clock (testing). */
    private final LongSupplier clock;

    public AdaptiveLimiter() { this(System::nanoTime); }

    AdaptiveLimiter(LongSupplier clock) {
        this.clock = clock;
        long sentinel = Long.MIN_VALUE / 2;
        for (int i = 0; i < bucket.length; i++) bucket[i] = sentinel;
    }

    /**
     * Attempt to begin one monitoring call. Callers that succeed MUST call
     * {@link #release()} in a {@code finally} block when the call returns (or
     * throws). Callers that fail must skip the tick.
     */
    public boolean tryAcquire() {
        long now = clock.getAsLong();
        long lastRel = lastReleaseNanos.get();
        if (lastRel != Long.MIN_VALUE / 2 && (now - lastRel) < MIN_GAP_MS * 1_000_000L) {
            return false;
        }
        bucketLock.lock();
        try {
            long oldest = bucket[bucketHead];
            if (oldest != Long.MIN_VALUE / 2 && (now - oldest) < 1_000_000_000L) {
                return false;
            }
            if (!inFlight.tryAcquire()) return false;
            bucket[bucketHead] = now;
            bucketHead = (bucketHead + 1) % bucket.length;
            return true;
        } finally {
            bucketLock.unlock();
        }
    }

    /** Release the in-flight permit. Must be paired with every successful acquire. */
    public void release() {
        lastReleaseNanos.set(clock.getAsLong());
        inFlight.release();
    }

    /** Number of permits currently in-flight (0 or 1). Exposed for diagnostics / tests. */
    public int inFlight() { return 1 - inFlight.availablePermits(); }

    @FunctionalInterface
    interface LongSupplier { long getAsLong(); }
}
