package com.kubrik.mex.migration.verify;

import java.time.Instant;
import java.util.List;

/** Result of post-migration verification (SAFE-5). Counts, sample diffs, index diffs, and —
 *  when {@code fullHashCompare} is enabled — a deterministic SHA-256 over the whole collection
 *  after applying the per-collection transform. */
public record VerificationReport(
        int schema,
        String jobId,
        Status status,
        List<CollectionReport> collections,
        Instant generatedAt,
        String engineVersion
) {

    public enum Status { PASS, WARN, FAIL }

    public record CollectionReport(
            String source,
            String target,
            long countSource,
            long countTarget,
            boolean countMatch,
            boolean transformed,
            int sampleSize,
            int sampleMismatches,
            List<String> indexDiff,
            String fullHash
    ) {}

    public static VerificationReport empty(String jobId, String engineVersion) {
        return new VerificationReport(
                1, jobId, Status.PASS, List.of(), Instant.now(), engineVersion);
    }
}
