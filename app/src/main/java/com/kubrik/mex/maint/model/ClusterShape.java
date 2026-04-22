package com.kubrik.mex.maint.model;

import java.util.Objects;

/**
 * v2.7 Q2.7-F — Input to the parameter recommender: the
 * cluster-shape features the tuning rationale keys off.
 *
 * <p>Kept intentionally small — if a field isn't load-bearing on
 * any recommendation, it stays out. {@code workload} is a coarse
 * label (OLTP / OLAP / MIXED) derived from v2.1 profile data
 * averages — the wizard fills it in from the monitoring subsystem.</p>
 */
public record ClusterShape(
        String storageEngine,   // "wiredTiger" / "inMemory" / …
        long ramBytes,          // physical host RAM
        int cpuCores,
        long docCountApprox,
        Workload workload,
        int serverMajorVersion  // e.g. 7 for 7.0.x
) {
    public enum Workload { OLTP, OLAP, MIXED, UNKNOWN }

    public ClusterShape {
        Objects.requireNonNull(storageEngine, "storageEngine");
        Objects.requireNonNull(workload, "workload");
        if (ramBytes < 0) throw new IllegalArgumentException("ramBytes negative");
        if (cpuCores < 0) throw new IllegalArgumentException("cpuCores negative");
    }
}
