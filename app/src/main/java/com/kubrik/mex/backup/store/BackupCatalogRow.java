package com.kubrik.mex.backup.store;

/**
 * v2.5 BKP-RUN-1..8 — a single {@code backup_catalog} row. Carries the
 * execution window, destination pointers (sink + path), and the artefact
 * roll-up (total bytes, doc count, oplog window). {@code manifestSha256} is
 * the footer hash computed by {@link com.kubrik.mex.backup.manifest.BackupManifest}
 * — it's the authoritative identity for this backup's tree.
 */
public record BackupCatalogRow(
        long id,
        Long policyId,
        String connectionId,
        long startedAt,
        Long finishedAt,
        BackupStatus status,
        long sinkId,
        String sinkPath,
        String manifestSha256,
        Long totalBytes,
        Long docCount,
        Long oplogFirstTs,
        Long oplogLastTs,
        Long verifiedAt,
        String verifyOutcome,
        String notes
) {
    public BackupCatalogRow {
        if (connectionId == null || connectionId.isBlank())
            throw new IllegalArgumentException("connectionId");
        if (status == null) throw new IllegalArgumentException("status");
        if (sinkPath == null || sinkPath.isBlank())
            throw new IllegalArgumentException("sinkPath");
    }

    public BackupCatalogRow withId(long newId) {
        return new BackupCatalogRow(newId, policyId, connectionId, startedAt, finishedAt,
                status, sinkId, sinkPath, manifestSha256, totalBytes, docCount,
                oplogFirstTs, oplogLastTs, verifiedAt, verifyOutcome, notes);
    }
}
