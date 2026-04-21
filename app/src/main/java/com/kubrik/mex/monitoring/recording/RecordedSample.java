package com.kubrik.mex.monitoring.recording;

/**
 * One row loaded from {@code recording_samples}. {@code labelsJson} is the canonical
 * JSON form produced by {@link com.kubrik.mex.monitoring.model.LabelSet#toJson()};
 * consumers that need the parsed map can use
 * {@link com.kubrik.mex.monitoring.store.LabelSetJson#parse(String)}.
 */
public record RecordedSample(long tsMs, String labelsJson, double value) {}
