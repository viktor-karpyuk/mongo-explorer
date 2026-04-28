package com.kubrik.mex.backup.event;

/**
 * v2.5 Q2.5-E — restore lifecycle event published by {@code RestoreService}.
 *
 * <p>Three event kinds, shaped to match {@link BackupEvent}:</p>
 * <ul>
 *   <li>{@link Started} — restore begun, audit row written.</li>
 *   <li>{@link Progress} — mongorestore progress tick (docs + current ns).</li>
 *   <li>{@link Ended} — terminal state with success + failure counters.</li>
 * </ul>
 */
public sealed interface RestoreEvent {

    long catalogId();
    String connectionId();

    record Started(long catalogId, String connectionId, String mode, long startedAt)
            implements RestoreEvent {}

    record Progress(long catalogId, String connectionId, long docsRestored,
                    String currentNs, long failuresSoFar) implements RestoreEvent {}

    record Ended(long catalogId, String connectionId, boolean ok, long durationMs,
                 long failures, String message) implements RestoreEvent {}
}
