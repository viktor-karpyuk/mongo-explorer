package com.kubrik.mex.backup.spec;

/**
 * v2.5 BKP-POLICY-6 — archive options for a backup run. When
 * {@link #gzip} is false the runner writes raw bson files; when true it
 * shells out to {@code mongodump --gzip} and {@link #level} is the gzip
 * level (1..9, 1 = fastest, 9 = smallest). {@code outputDirTemplate}
 * names the folder under the sink root; substitution syntax is defined
 * with the runner (Q2.5-B).
 */
public record ArchiveSpec(boolean gzip, int level, String outputDirTemplate) {

    public static final String DEFAULT_TEMPLATE = "<policy>/<yyyy-MM-dd_HH-mm-ss>";

    public ArchiveSpec {
        if (gzip && (level < 1 || level > 9))
            throw new IllegalArgumentException("gzip level must be 1..9, got " + level);
        if (outputDirTemplate == null || outputDirTemplate.isBlank())
            outputDirTemplate = DEFAULT_TEMPLATE;
    }

    public static ArchiveSpec defaults() {
        return new ArchiveSpec(true, 6, DEFAULT_TEMPLATE);
    }
}
