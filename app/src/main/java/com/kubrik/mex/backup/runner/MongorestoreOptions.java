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
        int parallelCollections
) {
    public MongorestoreOptions {
        if (uri == null || uri.isBlank()) throw new IllegalArgumentException("uri");
        if (sourceDir == null) throw new IllegalArgumentException("sourceDir");
        nsRename = nsRename == null ? Map.of() : Map.copyOf(nsRename);
        if (parallelCollections < 1) parallelCollections = 4;
    }
}
