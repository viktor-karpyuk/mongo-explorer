package com.kubrik.mex.backup.store;

/**
 * v2.5 BKP-RUN-5 — one artefact file inside a backup tree.
 *
 * <p>{@code kind} classifies the file for the restore + verify code paths:
 * {@code "bson"} (data), {@code "metadata"} (collection / index metadata JSON),
 * {@code "oplog"} (oplog slice), {@code "manifest"} (the run's manifest). The
 * Q2.5-D verifier uses {@code kind} to decide which files must round-trip
 * byte-equal vs. which are advisory.</p>
 */
public record BackupFileRow(
        long id,
        long catalogId,
        String relativePath,
        long bytes,
        String sha256,
        String db,
        String coll,
        String kind
) {
    public BackupFileRow {
        if (relativePath == null || relativePath.isBlank())
            throw new IllegalArgumentException("relativePath");
        if (sha256 == null || sha256.length() != 64)
            throw new IllegalArgumentException("sha256 must be a 64-char hex string");
        if (kind == null || kind.isBlank())
            throw new IllegalArgumentException("kind");
        if (bytes < 0) throw new IllegalArgumentException("bytes");
    }

    public BackupFileRow withId(long newId) {
        return new BackupFileRow(newId, catalogId, relativePath, bytes, sha256,
                db, coll, kind);
    }
}
