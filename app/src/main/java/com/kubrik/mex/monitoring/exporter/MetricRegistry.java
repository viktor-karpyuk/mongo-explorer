package com.kubrik.mex.monitoring.exporter;

import com.kubrik.mex.monitoring.model.LabelSet;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps the latest value per (connection, metric, labels) tuple for the
 * Prometheus exporter. Updated in-place on every sample; {@link #snapshot()}
 * returns an immutable point-in-time view.
 */
public final class MetricRegistry {

    public record Snapshot(String connectionId, MetricId metric, LabelSet labels, long tsMs, double value) {}

    private final Map<Key, Snapshot> latest = new ConcurrentHashMap<>();

    public void onSamples(List<MetricSample> batch) {
        for (MetricSample s : batch) {
            Key k = new Key(s.connectionId(), s.metric(), s.labels());
            latest.put(k, new Snapshot(s.connectionId(), s.metric(), s.labels(), s.tsMs(), s.value()));
        }
    }

    /** Sorted snapshot — metric first, then labels. Prometheus appreciates stable order. */
    public List<Snapshot> snapshot() {
        List<Snapshot> out = new ArrayList<>(latest.values());
        out.sort((a, b) -> {
            int m = a.metric.name().compareTo(b.metric.name());
            if (m != 0) return m;
            int c = a.connectionId.compareTo(b.connectionId);
            if (c != 0) return c;
            return a.labels.toJson().compareTo(b.labels.toJson());
        });
        return out;
    }

    public void clearConnection(String connectionId) {
        latest.keySet().removeIf(k -> k.connectionId.equals(connectionId));
    }

    private record Key(String connectionId, MetricId metric, LabelSet labels) {}
}
