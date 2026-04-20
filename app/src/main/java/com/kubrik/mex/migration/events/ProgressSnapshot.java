package com.kubrik.mex.migration.events;

import java.time.Duration;
import java.util.List;

/** Aggregate progress for a single job. Published every ≤ 200 ms during a run
 *  (see docs/mvp-technical-spec.md §10.3).
 *
 *  <p>OBS-5: {@code docsProcessed} tracks documents that cleared the reader + transformer
 *  pipeline. In RUN mode it mirrors {@code docsCopied}; in DRY_RUN mode it advances while
 *  {@code docsCopied} stays 0 (no writes) — the UI binds the "Docs" column to whichever
 *  matches the execution mode so dry-runs don't appear stuck at zero. */
public record ProgressSnapshot(
        long docsCopied,
        long docsProcessed,          // OBS-5
        long docsTotal,              // -1 when unknown
        long bytesCopied,
        double docsPerSecRolling,    // 10-second sliding window
        double mbPerSecRolling,      // 10-second sliding window
        Duration elapsed,
        Duration eta,                // Duration.ZERO when unknown
        List<CollectionProgress> perCollection,
        long errors
) {}
