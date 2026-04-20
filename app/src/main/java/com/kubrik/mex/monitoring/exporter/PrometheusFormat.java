package com.kubrik.mex.monitoring.exporter;

import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.Unit;

import java.util.List;

/**
 * Renders a list of {@link MetricRegistry.Snapshot}s into Prometheus text-format
 * (v0.0.4). Metric names use the {@code mex_mongo_...} prefix per EXPORT-4 and
 * units follow Prometheus base-unit conventions per EXPORT-5.
 */
public final class PrometheusFormat {

    private PrometheusFormat() {}

    public static String render(List<MetricRegistry.Snapshot> snaps) {
        StringBuilder sb = new StringBuilder(16 * 1024);
        MetricId currentMetric = null;
        for (MetricRegistry.Snapshot s : snaps) {
            if (s.metric() != currentMetric) {
                currentMetric = s.metric();
                String name = promName(currentMetric);
                sb.append("# HELP ").append(name).append(' ').append(help(currentMetric)).append('\n');
                sb.append("# TYPE ").append(name).append(' ').append(type(currentMetric)).append('\n');
            }
            sb.append(promName(s.metric()));
            sb.append('{');
            sb.append("connection=\"").append(escape(s.connectionId())).append('"');
            for (var e : s.labels().labels().entrySet()) {
                sb.append(',').append(e.getKey()).append("=\"").append(escape(e.getValue())).append('"');
            }
            sb.append("} ").append(promValue(s.metric(), s.value())).append(' ').append(s.tsMs()).append('\n');
        }
        return sb.toString();
    }

    static String promName(MetricId id) {
        // Target form: mex_mongo_<workstream>_<metric> per EXPORT-4.
        // Metric names like "mongo.ops.insert" already carry the mongo prefix;
        // others (e.g. "wt.cache.fill_ratio", "repl.member.state") do not, so
        // add it explicitly.
        String n = id.metricName().replace('.', '_');
        return n.startsWith("mongo_") ? "mex_" + n : "mex_mongo_" + n;
    }

    static String type(MetricId id) {
        return switch (id.unit()) {
            case OPS_PER_SECOND, BYTES_PER_SECOND -> "gauge"; // stored as rates already
            default -> "gauge";
        };
    }

    /** EXPORT-5 — convert µs / ms to seconds where the unit is time. */
    static double promValue(MetricId id, double raw) {
        return switch (id.unit()) {
            case MICROSECONDS -> raw / 1_000_000.0;
            case MILLISECONDS -> raw / 1_000.0;
            default -> raw;
        };
    }

    static String help(MetricId id) {
        return id.metricName() + " (" + id.unit() + ")";
    }

    private static String escape(String v) {
        if (v == null) return "";
        StringBuilder sb = new StringBuilder(v.length());
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '"'  -> sb.append("\\\"");
                case '\n' -> sb.append("\\n");
                default   -> sb.append(c);
            }
        }
        return sb.toString();
    }
}
