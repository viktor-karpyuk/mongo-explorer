package com.kubrik.mex.monitoring.model;

import java.time.Duration;

/** Time-series storage tier. See requirements.md §9 (HIST-*) and technical-spec §5.3. */
public enum RollupTier {
    RAW("metric_samples_raw", Duration.ofSeconds(1),  Duration.ofHours(24)),
    S10("metric_samples_10s", Duration.ofSeconds(10), Duration.ofDays(7)),
    M1 ("metric_samples_1m",  Duration.ofMinutes(1),  Duration.ofDays(90)),
    H1 ("metric_samples_1h",  Duration.ofHours(1),    Duration.ofDays(365));

    private final String tableName;
    private final Duration windowSize;
    private final Duration defaultHorizon;

    RollupTier(String tableName, Duration windowSize, Duration defaultHorizon) {
        this.tableName = tableName;
        this.windowSize = windowSize;
        this.defaultHorizon = defaultHorizon;
    }

    public String tableName() { return tableName; }
    public Duration windowSize() { return windowSize; }
    public Duration defaultHorizon() { return defaultHorizon; }
}
