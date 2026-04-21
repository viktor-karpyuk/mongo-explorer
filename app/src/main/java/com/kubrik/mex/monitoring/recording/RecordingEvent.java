package com.kubrik.mex.monitoring.recording;

/**
 * Pub/sub event emitted on {@code EventBus.onRecording(...)}. See technical-spec §9.
 *
 * <p>Delivery is synchronous on the producer thread — UI consumers must re-post to
 * the JavaFX application thread via {@code Platform.runLater} before touching any
 * node. There is no ordering guarantee between {@link Started} and the first
 * captured sample landing in {@code recording_samples}, so consumers read
 * {@code RecordingService} state for truth; events drive refresh.
 */
public sealed interface RecordingEvent permits
        RecordingEvent.Started,
        RecordingEvent.Paused,
        RecordingEvent.Resumed,
        RecordingEvent.Stopped,
        RecordingEvent.SampleWriteError {

    String recordingId();
    long tsMs();

    record Started(String recordingId, String connectionId, long tsMs) implements RecordingEvent {}
    record Paused(String recordingId, long tsMs) implements RecordingEvent {}
    record Resumed(String recordingId, long tsMs) implements RecordingEvent {}
    record Stopped(String recordingId, StopReason reason, long tsMs) implements RecordingEvent {}
    record SampleWriteError(String recordingId, String msg, long tsMs) implements RecordingEvent {}
}
