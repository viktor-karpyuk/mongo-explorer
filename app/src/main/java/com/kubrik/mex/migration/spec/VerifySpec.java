package com.kubrik.mex.migration.spec;

/** Post-migration verification options (SAFE-5, SAFE-6). */
public record VerifySpec(
        boolean enabled,
        int sample,
        boolean fullHashCompare
) {
    public static VerifySpec defaults() {
        return new VerifySpec(true, 1_000, false);
    }
}
