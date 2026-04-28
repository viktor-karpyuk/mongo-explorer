package com.kubrik.mex.backup.manifest;

/**
 * v2.5 BKP-RUN-4 / BKP-RUN-5 — oplog tail recorded alongside a backup. The
 * Q2.5-F PITR picker walks the set of catalog rows that cover a target
 * timestamp using {@code firstTs} / {@code lastTs}; if no slice covers the
 * target, the restore plan refuses to render.
 */
public record OplogSlice(long firstTs, long lastTs, String path, String sha256) {
    public OplogSlice {
        if (firstTs < 0 || lastTs < 0) throw new IllegalArgumentException("ts");
        if (lastTs < firstTs) throw new IllegalArgumentException("lastTs < firstTs");
        if (path == null || path.isBlank()) throw new IllegalArgumentException("path");
        if (sha256 == null || sha256.length() != 64)
            throw new IllegalArgumentException("sha256 must be 64 hex chars");
    }
}
