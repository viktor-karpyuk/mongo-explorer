package com.kubrik.mex.cluster.safety;

import org.bson.Document;

/**
 * v2.4 SAFE-OPS-1 — output of a {@link com.kubrik.mex.cluster.dryrun.DryRunRenderer}.
 * The {@code commandJson} is byte-stable (canonical JSON) so
 * {@link #previewHash} is reproducible across environments.
 */
public record DryRunResult(
        String commandName,
        Document commandBson,
        String commandJson,
        String summary,
        String predictedEffect,
        String previewHash
) {
    public DryRunResult {
        if (commandName == null || commandName.isBlank()) throw new IllegalArgumentException("commandName");
        if (commandBson == null) throw new IllegalArgumentException("commandBson");
        if (commandJson == null) throw new IllegalArgumentException("commandJson");
        if (previewHash == null || previewHash.length() != 64) throw new IllegalArgumentException("previewHash");
        if (summary == null) summary = "";
        if (predictedEffect == null) predictedEffect = "";
    }
}
