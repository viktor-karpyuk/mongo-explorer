package com.kubrik.mex.migration.spec;

/** Toggles applied to every {@link ScopeSpec} variant. Kept as a small record (rather than
 *  a flag-per-field on each variant) so new flags land in one place. */
public record ScopeFlags(boolean migrateIndexes, boolean migrateUsers) {
    public static ScopeFlags defaults() { return new ScopeFlags(true, false); }

    public static ScopeFlags indexesOnly() { return new ScopeFlags(true, false); }
}
