package com.kubrik.mex.monitoring.model;

import java.util.Objects;

/**
 * One observed value of a metric at a point in time. Bool metrics encode true=1.0,
 * false=0.0; enum metrics encode via a stable integer mapping.
 */
public record MetricSample(
        String connectionId,
        MetricId metric,
        LabelSet labels,
        long tsMs,
        double value
) {
    public MetricSample {
        Objects.requireNonNull(connectionId, "connectionId");
        Objects.requireNonNull(metric, "metric");
        Objects.requireNonNull(labels, "labels");
        if (tsMs <= 0) throw new IllegalArgumentException("tsMs must be positive");
    }
}
