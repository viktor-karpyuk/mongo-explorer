package com.kubrik.mex.monitoring.recording;

/**
 * One unit of work for the capture writer thread — a sample bound for
 * {@code recording_samples}. Emitted by {@code RecordingCaptureSubscriber}.
 */
public record RecordingWriteTask(
        String recordingId,
        String connectionId,
        String metric,
        String labelsJson,
        long tsMs,
        double value
) {}
