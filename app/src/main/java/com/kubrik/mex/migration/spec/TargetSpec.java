package com.kubrik.mex.migration.spec;

/** Target-side of a migration spec. `database` is required for VERSIONED migrations and
 *  optional for DATA_TRANSFER (overridable per-collection via renames). */
public record TargetSpec(
        String connectionId,
        String database
) {
    public static TargetSpec of(String connectionId) {
        return new TargetSpec(connectionId, null);
    }
}
