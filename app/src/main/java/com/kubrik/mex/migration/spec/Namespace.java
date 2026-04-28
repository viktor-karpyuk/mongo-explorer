package com.kubrik.mex.migration.spec;

/** Fully-qualified MongoDB namespace — database + collection. SCOPE-11 lets a single job
 *  target collections across multiple databases, so the explicit-list form of
 *  {@link ScopeSpec.Collections} keys on namespaces rather than bare names. */
public record Namespace(String db, String coll) {

    /** Parse a {@code db.coll} string. Collection names may contain dots — only the first
     *  dot is treated as the separator. */
    public static Namespace parse(String dotted) {
        int i = dotted.indexOf('.');
        if (i <= 0 || i == dotted.length() - 1) {
            throw new IllegalArgumentException("not a valid namespace: " + dotted);
        }
        return new Namespace(dotted.substring(0, i), dotted.substring(i + 1));
    }

    /** Render as {@code db.coll}. */
    public String dotted() { return db + "." + coll; }

    @Override public String toString() { return dotted(); }
}
