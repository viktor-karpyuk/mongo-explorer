package com.kubrik.mex.backup.runner;

import java.nio.file.Path;
import java.util.Map;

/**
 * v2.5 Q2.5-E — concrete invocation parameters for {@code mongorestore}.
 *
 * <p>{@code nsRename} maps source namespaces to target ones (mongorestore's
 * {@code --nsFrom / --nsTo} pairs); {@code dropBeforeRestore} maps to
 * {@code --drop}; {@code dryRun} to mongorestore's {@code --dryRun} flag
 * (available since 4.4). The wizard's Rehearse mode uses nsRename to route
 * into an isolated sandbox DB without touching production names.</p>
 */
public record MongorestoreOptions(
        String uri,
        Path sourceDir,
        Map<String, String> nsRename,
        boolean dropBeforeRestore,
        boolean dryRun,
        boolean gzip,
        boolean oplogReplay,
        int parallelCollections,
        /** v2.6 Q2.6-L5 — oplog replay cut-off timestamp in seconds.
         *  When non-null (and {@link #oplogReplay} is true), mongorestore
         *  stops applying oplog entries at this BSON-timestamp (t:ord).
         *  Set by the PITR wizard from {@code PitrPlanner#oplogLimitTs}.
         *  Null means "replay the entire slice" (pre-v2.6 behaviour). */
        Long oplogLimitTsSecs
) {
    public MongorestoreOptions {
        if (uri == null || uri.isBlank()) throw new IllegalArgumentException("uri");
        if (sourceDir == null) throw new IllegalArgumentException("sourceDir");
        nsRename = nsRename == null ? Map.of() : Map.copyOf(nsRename);
        if (parallelCollections < 1) parallelCollections = 4;
    }

    /** Back-compat constructor for callers that don't care about oplogLimit. */
    public MongorestoreOptions(String uri, Path sourceDir, Map<String, String> nsRename,
                                boolean dropBeforeRestore, boolean dryRun, boolean gzip,
                                boolean oplogReplay, int parallelCollections) {
        this(uri, sourceDir, nsRename, dropBeforeRestore, dryRun, gzip,
                oplogReplay, parallelCollections, null);
    }
}
