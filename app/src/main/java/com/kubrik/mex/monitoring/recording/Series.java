package com.kubrik.mex.monitoring.recording;

/**
 * Distinct {@code (metric, labels_json)} tuple captured inside a recording, together
 * with the count of samples that make it up. Populated by
 * {@code RecordingSampleDao.listSeries}.
 */
public record Series(String metric, String labelsJson, long sampleCount) {}
