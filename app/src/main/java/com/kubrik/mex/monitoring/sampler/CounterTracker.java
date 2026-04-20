package com.kubrik.mex.monitoring.sampler;

import com.kubrik.mex.monitoring.model.LabelSet;
import com.kubrik.mex.monitoring.model.MetricId;

import java.util.OptionalDouble;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Converts cumulative MongoDB counters (ops, bytes, asserts) into per-second rates.
 * The first sample for any key is stored but not emitted — the second sample
 * produces the first rate. A decrease in the cumulative value is treated as a
 * server restart: the tracker discards the key and starts over.
 *
 * <p>See technical-spec §4.3. Thread-safe.
 */
public final class CounterTracker {

    private final ConcurrentHashMap<Key, PrevSample> last = new ConcurrentHashMap<>();

    public OptionalDouble rate(String connectionId, MetricId metric, LabelSet labels,
                               long tsMs, double cumulative) {
        Key key = new Key(connectionId, metric, labels);
        PrevSample[] prev = new PrevSample[1];
        last.compute(key, (k, v) -> {
            prev[0] = v;
            if (v == null || cumulative < v.value) {
                return new PrevSample(tsMs, cumulative);
            }
            return new PrevSample(tsMs, cumulative);
        });
        PrevSample p = prev[0];
        if (p == null) return OptionalDouble.empty();
        if (cumulative < p.value) return OptionalDouble.empty();
        double dtSec = (tsMs - p.tsMs) / 1000.0;
        if (dtSec <= 0) return OptionalDouble.of(0.0);
        return OptionalDouble.of((cumulative - p.value) / dtSec);
    }

    /** Drop all tracked state for a connection (e.g. on disconnect). */
    public void forgetConnection(String connectionId) {
        last.keySet().removeIf(k -> k.connectionId.equals(connectionId));
    }

    int size() { return last.size(); }

    private record Key(String connectionId, MetricId metric, LabelSet labels) {}
    private record PrevSample(long tsMs, double value) {}
}
