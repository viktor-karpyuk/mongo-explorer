package com.kubrik.mex.monitoring.sampler;

import com.kubrik.mex.monitoring.model.MetricId;

import java.util.OptionalDouble;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@code opLatencies.{reads|writes|commands|transactions}} exposes a cumulative
 * (total µs, total ops) pair. Average latency for a window = Δtotal_us / Δops.
 *
 * <p>This tracker holds the previous pair per metric; first call returns empty,
 * subsequent calls return the ratio in microseconds. Reset on any negative diff.
 */
public final class LatencyTracker {

    private final ConcurrentHashMap<MetricId, long[]> prev = new ConcurrentHashMap<>();

    /** Returns avg latency in microseconds over the interval since the last call. */
    public OptionalDouble avgLatencyUs(MetricId key, long totalLatencyUs, long totalOps) {
        long[] old = prev.put(key, new long[] { totalLatencyUs, totalOps });
        if (old == null) return OptionalDouble.empty();
        long dLat = totalLatencyUs - old[0];
        long dOps = totalOps - old[1];
        if (dLat < 0 || dOps < 0) return OptionalDouble.empty();
        if (dOps == 0) return OptionalDouble.of(0.0);
        return OptionalDouble.of((double) dLat / dOps);
    }
}
