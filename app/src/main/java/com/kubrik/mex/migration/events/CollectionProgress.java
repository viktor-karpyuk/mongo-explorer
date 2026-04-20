package com.kubrik.mex.migration.events;

/** Per-collection progress snapshot. `docsTotal` is -1 when unknown
 *  (see docs/mvp-functional-spec.md §6.6). OBS-5 adds {@code docsProcessed} so the UI can
 *  show a non-zero counter during dry-runs where {@code docsCopied} stays 0. */
public record CollectionProgress(
        String source,
        String target,
        long docsCopied,
        long docsProcessed,   // OBS-5
        long docsTotal,
        String status,
        int activePartitions
) {}
