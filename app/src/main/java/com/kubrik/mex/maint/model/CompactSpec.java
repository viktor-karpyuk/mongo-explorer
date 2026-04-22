package com.kubrik.mex.maint.model;

import java.util.List;
import java.util.Objects;

/**
 * v2.7 Q2.7-E — Parameters for a compact or resync operation.
 * Deliberately separate record per operation — the two share a
 * wizard UI but their server-side semantics diverge.
 */
public final class CompactSpec {

    public record Compact(
            String targetHost,
            String db,
            List<String> collections,
            boolean takeOutOfRotation,
            boolean force
    ) {
        public Compact {
            Objects.requireNonNull(targetHost, "targetHost");
            Objects.requireNonNull(db, "db");
            collections = List.copyOf(collections);
            if (collections.isEmpty())
                throw new IllegalArgumentException("compact needs ≥ 1 collection");
        }
    }

    public record Resync(
            String targetHost,
            boolean waitForCompletion
    ) {
        public Resync {
            Objects.requireNonNull(targetHost, "targetHost");
        }
    }

    private CompactSpec() {}
}
