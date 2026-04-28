package com.kubrik.mex.backup.event;

/**
 * v2.5 BKP-RUN-4 — lifecycle event published by the backup runner.
 *
 * <p>Three event kinds:</p>
 * <ul>
 *   <li>{@link Started} — run begun, catalog row is {@code RUNNING}.</li>
 *   <li>{@link Progress} — runner heartbeat (bytes + docs so far, current
 *       namespace). Frame-coalesced on the JavaFX pulse so the UI sees
 *       progress at display cadence, not mongodump's line rate.</li>
 *   <li>{@link Ended} — terminal state; {@code catalog} row has been
 *       finalised with status + manifest sha256.</li>
 * </ul>
 */
public sealed interface BackupEvent {

    long catalogId();
    String connectionId();

    record Started(long catalogId, String connectionId, long startedAt) implements BackupEvent {}

    record Progress(long catalogId, String connectionId,
                    long bytesWritten, long docsCopied, String currentNs)
            implements BackupEvent {}

    record Ended(long catalogId, String connectionId,
                com.kubrik.mex.backup.store.BackupStatus status,
                String manifestSha256, long totalBytes, long docCount,
                String message)
            implements BackupEvent {}
}
