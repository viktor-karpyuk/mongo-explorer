package com.kubrik.mex.migration.versioned;

import java.nio.file.Path;
import java.util.List;

/** A parsed versioned-migration script. Identity is {@code version}; {@code orderKey}
 *  provides a stable numeric sort, avoiding the lexicographic trap
 *  {@code "1.0.10" < "1.0.2"} (T-9). */
public record MigrationScript(
        String version,
        long orderKey,
        String description,
        String checksum,        // sha256:…
        List<Op> ops,
        Path source,            // disk file for diagnostics
        String envFilter        // VER-8 — e.g. "prod" or "!prod"; null = runs everywhere
) {

    /** True if this script should execute against a spec with the given environment value
     *  (possibly null). Matching is case-insensitive. The negated form ({@code !X}) runs
     *  against every environment except {@code X}. */
    public boolean runsIn(String specEnvironment) {
        if (envFilter == null || envFilter.isBlank()) return true;
        boolean negated = envFilter.startsWith("!");
        String target = (negated ? envFilter.substring(1) : envFilter).trim();
        boolean equal = target.equalsIgnoreCase(specEnvironment);
        return negated ? !equal : equal;
    }

    /** Parse a dotted-numeric version into a packed long used for sort.
     *  Up to 4 segments × 4 decimal digits each, clamped. */
    public static long computeOrderKey(String version) {
        String[] parts = version.split("\\.");
        long key = 0L;
        for (int i = 0; i < 4; i++) {
            long seg = 0;
            if (i < parts.length) {
                try { seg = Long.parseLong(parts[i]); }
                catch (NumberFormatException e) { seg = 0; }
            }
            key = key * 10_000L + Math.min(9_999, Math.max(0, seg));
        }
        return key;
    }
}
