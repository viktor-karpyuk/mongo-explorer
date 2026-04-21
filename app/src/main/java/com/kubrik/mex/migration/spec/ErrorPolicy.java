package com.kubrik.mex.migration.spec;

/** Per-document error handling (REL-2). */
public record ErrorPolicy(
        int maxErrors,
        double maxErrorRate
) {
    public static ErrorPolicy defaults() {
        return new ErrorPolicy(100, 0.01);
    }
}
