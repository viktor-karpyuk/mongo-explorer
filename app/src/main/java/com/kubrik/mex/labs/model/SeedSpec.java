package com.kubrik.mex.labs.model;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.4 LAB-SEED-* — Template-level seed declaration. Either a
 * bundled tiny dataset (kept in {@code src/main/resources/labs/seeds})
 * or a fetch-on-demand larger one (the app install stays lean).
 */
public record SeedSpec(
        Kind kind,
        /** Bundled resource path inside the jar, OR remote URL for
         *  {@link Kind#FETCH_ON_DEMAND}. */
        String locator,
        /** Target database inside the Lab's mongod. */
        String targetDb,
        /** SHA-256 of the expected archive content; verified for
         *  fetch-on-demand, empty for bundled (classpath bytes
         *  are trusted). */
        Optional<String> sha256
) {
    public SeedSpec {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(locator, "locator");
        Objects.requireNonNull(targetDb, "targetDb");
        sha256 = sha256 == null ? Optional.empty() : sha256;
    }

    public enum Kind { BUNDLED, FETCH_ON_DEMAND }
}
