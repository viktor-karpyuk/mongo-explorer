package com.kubrik.mex.monitoring.sampler;

import java.time.Duration;

/**
 * Per-sampler SAFE-3 back-off: when the most recent {@link #WINDOW} monitoring-call
 * latencies include any above {@link #SLOW_THRESHOLD_MS}, the scheduler doubles
 * the effective poll interval for this sampler. Recovery: three consecutive polls
 * under {@link #RECOVERY_THRESHOLD_MS} return the sampler to normal cadence.
 *
 * <p>Not thread-safe; each sampler loop owns its own instance.
 */
public final class BackoffTracker {

    public static final int WINDOW = 10;
    public static final long SLOW_THRESHOLD_MS = 100;
    public static final long RECOVERY_THRESHOLD_MS = 50;
    public static final int RECOVERY_STREAK = 3;

    private final long[] recent = new long[WINDOW];
    private int recentHead = 0;
    private int recentCount = 0;

    private int fastStreak = 0;
    private boolean backedOff = false;

    /** Record the most recent monitoring-call latency (ms). */
    public void record(long latencyMs) {
        recent[recentHead] = latencyMs;
        recentHead = (recentHead + 1) % WINDOW;
        if (recentCount < WINDOW) recentCount++;

        if (latencyMs < RECOVERY_THRESHOLD_MS) {
            fastStreak++;
        } else {
            fastStreak = 0;
        }

        if (backedOff) {
            if (fastStreak >= RECOVERY_STREAK) backedOff = false;
        } else {
            if (anyRecentAboveSlowThreshold()) backedOff = true;
        }
    }

    public boolean isBackedOff() { return backedOff; }

    /** Effective poll interval: 2× base when backed-off, otherwise {@code base}. */
    public Duration effectiveInterval(Duration base) {
        return backedOff ? base.multipliedBy(2) : base;
    }

    private boolean anyRecentAboveSlowThreshold() {
        for (int i = 0; i < recentCount; i++) {
            if (recent[i] > SLOW_THRESHOLD_MS) return true;
        }
        return false;
    }
}
