package com.kubrik.mex.monitoring.store;

/**
 * Persisted slow-query sample. See technical-spec §8.2 and requirements.md §6.3.
 * {@code commandJson} is already redacted — the raw form is never stored (BR-9).
 */
public record ProfileSampleRecord(
        String connectionId,
        long tsMs,
        String ns,
        String op,
        long millis,
        String planSummary,
        Long docsExamined,
        Long docsReturned,
        Long keysExamined,
        Long numYield,
        Long responseLength,
        String queryHash,
        String planCacheKey,
        String commandJson
) {}
