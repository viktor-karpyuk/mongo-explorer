package com.kubrik.mex.cluster.ops;

/**
 * v2.4 OPLOG-1..4 — read-only snapshot of the local.oplog.rs gauge. {@code
 * firstTsSec} / {@code lastTsSec} are epoch-seconds extracted from the wire
 * timestamps of the oldest and newest oplog entry; {@code windowHours} is
 * their difference in hours (0 when the oplog is empty or the sample failed).
 */
public record OplogGaugeStats(
        boolean supported,
        long sizeBytes,
        long usedBytes,
        long firstTsSec,
        long lastTsSec,
        double windowHours,
        String errorMessage
) {
    public static OplogGaugeStats unsupported(String why) {
        return new OplogGaugeStats(false, 0L, 0L, 0L, 0L, 0.0, why);
    }

    public double usageRatio() {
        return sizeBytes <= 0 ? 0.0 : (double) usedBytes / sizeBytes;
    }

    public String band() {
        if (windowHours < 6) return "red";
        if (windowHours < 24) return "amber";
        return "green";
    }
}
