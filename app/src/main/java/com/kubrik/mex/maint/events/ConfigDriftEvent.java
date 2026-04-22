package com.kubrik.mex.maint.events;

import java.util.List;
import java.util.Objects;

/**
 * v2.7 DRIFT-CFG — Event-bus payload for config drift detection.
 * Emitted by the snapshot scheduler when a new snapshot's hash
 * differs from the previous for the same (connection, host, kind)
 * triple.
 */
public record ConfigDriftEvent(
        String connectionId,
        String host,
        String kind,
        String priorSha256,
        String currentSha256,
        List<String> changedPaths,
        long detectedAt
) {
    public ConfigDriftEvent {
        Objects.requireNonNull(connectionId);
        Objects.requireNonNull(kind);
        changedPaths = List.copyOf(changedPaths);
    }
}
