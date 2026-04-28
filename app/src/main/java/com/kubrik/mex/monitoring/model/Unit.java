package com.kubrik.mex.monitoring.model;

/** Display + export unit for a metric. See v2.1.0 technical-spec §3.1. */
public enum Unit {
    MICROSECONDS,
    MILLISECONDS,
    BYTES,
    BYTES_PER_SECOND,
    OPS_PER_SECOND,
    RATIO,
    COUNT,
    BOOL,
    TIMESTAMP_MS
}
