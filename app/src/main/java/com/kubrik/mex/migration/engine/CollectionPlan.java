package com.kubrik.mex.migration.engine;

/** Resolved mapping from one source namespace to one target namespace, plus the conflict mode
 *  chosen for it. Produced by scope resolution, consumed by {@link CollectionPipeline}
 *  (for {@code COLLECTION}) or {@link com.kubrik.mex.migration.engine.ViewCreator}
 *  (for {@code VIEW}). */
public record CollectionPlan(
        String sourceNs,
        String targetNs,
        com.kubrik.mex.migration.spec.ConflictMode conflictMode,
        boolean isView
) {

    /** Back-compat factory — callers who only care about data-copy collections can skip the
     *  view flag and get the default ({@code false}). */
    public CollectionPlan(String sourceNs, String targetNs,
                          com.kubrik.mex.migration.spec.ConflictMode conflictMode) {
        this(sourceNs, targetNs, conflictMode, false);
    }

    public Namespaces.Ns source() { return Namespaces.parse(sourceNs); }
    public Namespaces.Ns target() { return Namespaces.parse(targetNs); }
}
