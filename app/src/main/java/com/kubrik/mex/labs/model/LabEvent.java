package com.kubrik.mex.labs.model;

import java.util.Objects;

/**
 * v2.8.4 — Row view of {@code lab_events}. Append-only lifecycle log
 * per Lab; UI tails it for the rollout viewer + history panel.
 * Deliberately separate from {@code ops_audit} (milestone §1.4 bullet
 * 5 — Labs are demo; compliance trail stays clean).
 */
public record LabEvent(
        long id,
        long labId,
        long at,
        Kind kind,
        String message  // nullable
) {
    public LabEvent {
        Objects.requireNonNull(kind, "kind");
    }

    public enum Kind {
        APPLY, START, STOP, DESTROY,
        HEALTHY, FAILED,
        SEED_BEGIN, SEED_DONE, SEED_FAILED,
        ADOPTED
    }
}
