package com.kubrik.mex.migration.spec;

/** Per-collection policy when the target namespace already has data.
 *  See docs/mvp-functional-spec.md §4.7 and docs/mvp-technical-spec.md §6.4. */
public enum ConflictMode {
    ABORT,
    APPEND,
    UPSERT_BY_ID,
    DROP_AND_RECREATE
}
