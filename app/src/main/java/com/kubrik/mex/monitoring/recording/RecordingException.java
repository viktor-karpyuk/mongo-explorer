package com.kubrik.mex.monitoring.recording;

/**
 * Sealed exception hierarchy for {@code RecordingService}. Using a sealed interface
 * of records (instead of a classic Throwable tree) lets callers switch exhaustively —
 * see technical-spec §3.3.
 *
 * <p>These are checked behaviourally at service boundaries but thrown as a wrapping
 * {@link RuntimeException} so they propagate cleanly through JavaFX handlers.
 */
public sealed interface RecordingException permits
        RecordingException.ConnectionNotConnected,
        RecordingException.RecordingAlreadyActive,
        RecordingException.StorageCapExceeded,
        RecordingException.RecordingNotFound,
        RecordingException.RecordingNotEditable {

    /** Raise this as a RuntimeException the UI can catch and map to a ME-23-* message. */
    default RuntimeException asRuntime() {
        return new RecordingServiceException(this);
    }

    record ConnectionNotConnected(String connectionId) implements RecordingException {}
    record RecordingAlreadyActive(String connectionId, String activeName) implements RecordingException {}
    record StorageCapExceeded(long usedBytes, long capBytes) implements RecordingException {}
    record RecordingNotFound(String recordingId) implements RecordingException {}
    record RecordingNotEditable(String recordingId, String why) implements RecordingException {}

    /** Runtime wrapper so a checked-style API stays ergonomic across JavaFX boundaries. */
    final class RecordingServiceException extends RuntimeException {
        private final RecordingException cause;
        RecordingServiceException(RecordingException cause) {
            super(cause.toString());
            this.cause = cause;
        }
        public RecordingException reason() { return cause; }
    }
}
