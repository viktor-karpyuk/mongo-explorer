package com.kubrik.mex.backup.manifest;

/**
 * v2.5 BKP-RUN-5 — one entry in the manifest's {@code files} array.
 * Hash is the hex-lowercase SHA-256 of the file's bytes.
 */
public record FileRecord(String path, long bytes, String sha256) {
    public FileRecord {
        if (path == null || path.isBlank()) throw new IllegalArgumentException("path");
        if (sha256 == null || sha256.length() != 64)
            throw new IllegalArgumentException("sha256 must be 64 hex chars");
        if (bytes < 0) throw new IllegalArgumentException("bytes");
    }
}
