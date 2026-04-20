package com.kubrik.mex.migration.spec;

/** Performance knobs (PERF-2…PERF-5, REL-1). Defaults documented in
 *  docs/mvp-functional-spec.md §6.4 and docs/mvp-technical-spec.md §20. */
public record PerfSpec(
        int parallelCollections,
        long partitionThreshold,
        int batchDocs,
        long batchBytes,
        long rateLimitDocsPerSec,
        int retryAttempts
) {
    public static PerfSpec defaults() {
        int cpu = Math.min(Runtime.getRuntime().availableProcessors(), 4);
        return new PerfSpec(
                cpu,
                1_000_000L,
                1_000,
                16L * 1024 * 1024,
                0L,
                5);
    }
}
