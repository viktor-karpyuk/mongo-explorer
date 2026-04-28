package com.kubrik.mex.migration.spec;

/** Source-side of a migration spec. Carries only the connection ID and read preference
 *  — credentials are resolved at run time via ConnectionStore + Crypto (SEC-1). */
public record SourceSpec(
        String connectionId,
        String readPreference          // primary | primaryPreferred | secondary | secondaryPreferred | nearest
) {
    public static SourceSpec of(String connectionId) {
        return new SourceSpec(connectionId, "primary");
    }
}
