package com.kubrik.mex.monitoring.sampler;

import java.time.Duration;

/**
 * Identifies a sampler family and declares its default poll cadence and read-preference
 * requirement. Matches technical-spec §4.2.
 */
public enum SamplerKind {
    SERVER_STATUS  (Duration.ofSeconds(1),   ReadPrefRequirement.ANY),
    DB_STATS       (Duration.ofSeconds(60),  ReadPrefRequirement.ANY),
    COLL_STATS     (Duration.ofSeconds(60),  ReadPrefRequirement.ANY),
    INDEX_STATS    (Duration.ofMinutes(5),   ReadPrefRequirement.ANY),
    REPL_STATUS    (Duration.ofSeconds(5),   ReadPrefRequirement.ANY),
    OPLOG          (Duration.ofSeconds(30),  ReadPrefRequirement.PRIMARY),
    SHARDING       (Duration.ofSeconds(30),  ReadPrefRequirement.ANY),
    TOP            (Duration.ofSeconds(5),   ReadPrefRequirement.PRIMARY),
    CURRENT_OP     (Duration.ofSeconds(5),   ReadPrefRequirement.PRIMARY),
    PROFILER       (Duration.ofSeconds(1),   ReadPrefRequirement.PRIMARY),
    METADATA       (Duration.ofHours(1),     ReadPrefRequirement.ANY);

    private final Duration defaultInterval;
    private final ReadPrefRequirement readPrefRequirement;

    SamplerKind(Duration defaultInterval, ReadPrefRequirement rpr) {
        this.defaultInterval = defaultInterval;
        this.readPrefRequirement = rpr;
    }

    public Duration defaultInterval() { return defaultInterval; }
    public ReadPrefRequirement readPrefRequirement() { return readPrefRequirement; }

    public enum ReadPrefRequirement { ANY, PRIMARY }
}
