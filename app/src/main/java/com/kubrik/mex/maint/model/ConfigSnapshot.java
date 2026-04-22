package com.kubrik.mex.maint.model;

import java.util.Objects;

/**
 * v2.7 DRIFT-CFG-* — Single snapshot row. Canonical JSON form +
 * SHA-256 so the diff engine reuses the v2.6 infrastructure.
 */
public record ConfigSnapshot(
        long id,
        String connectionId,
        long capturedAt,
        String host,              // null = cluster-wide
        Kind kind,
        String snapshotJson,
        String sha256
) {
    public enum Kind { PARAMETERS, CMDLINE, FCV, SHARDING }

    public ConfigSnapshot {
        Objects.requireNonNull(connectionId, "connectionId");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(snapshotJson, "snapshotJson");
        Objects.requireNonNull(sha256, "sha256");
    }
}
