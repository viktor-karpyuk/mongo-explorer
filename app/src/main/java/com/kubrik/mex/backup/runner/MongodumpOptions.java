package com.kubrik.mex.backup.runner;

import com.kubrik.mex.backup.spec.ArchiveSpec;
import com.kubrik.mex.backup.spec.Scope;

import java.nio.file.Path;

/**
 * v2.5 BKP-RUN-1..3 — concrete invocation parameters for {@code mongodump}.
 * Separated from {@link com.kubrik.mex.backup.spec.BackupPolicy} so the runner
 * stays unit-testable without a full policy + connection graph: construct a
 * fixture directly, feed it to {@link MongodumpRunner} with a mocked
 * {@link java.util.function.Function} process factory.
 */
public record MongodumpOptions(
        String uri,
        Path outDir,
        Scope scope,
        ArchiveSpec archive,
        boolean includeOplog,
        int parallelCollections
) {
    public MongodumpOptions {
        if (uri == null || uri.isBlank()) throw new IllegalArgumentException("uri");
        if (outDir == null) throw new IllegalArgumentException("outDir");
        if (scope == null) throw new IllegalArgumentException("scope");
        if (archive == null) throw new IllegalArgumentException("archive");
        if (parallelCollections < 1) parallelCollections = 4;
    }
}
