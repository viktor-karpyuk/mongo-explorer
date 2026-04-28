package com.kubrik.mex.maint.model;

import java.util.Objects;

/**
 * v2.7 Q2.7-B — Parsed form of a collection's
 * {@code $jsonSchema} validator plus the proposed rollout mode.
 *
 * <p>The editor loads the current validator via
 * {@code listCollections} / {@code collMod} introspection, lets the
 * user edit the JSON Schema, and wraps it in a {@link Rollout} when
 * Apply fires. The rollout becomes the {@code collMod} body.</p>
 */
public final class ValidatorSpec {

    public enum Level { OFF, MODERATE, STRICT }

    public enum Action { WARN, ERROR }

    /** Snapshot loaded from the server — immutable. */
    public record Current(
            String db,
            String coll,
            String validatorJson,
            Level level,
            Action action
    ) {
        public Current {
            Objects.requireNonNull(db, "db");
            Objects.requireNonNull(coll, "coll");
            Objects.requireNonNull(level, "level");
            Objects.requireNonNull(action, "action");
        }
    }

    /** The user's proposed change — what the wizard ships via
     *  {@code collMod}. */
    public record Rollout(
            String db,
            String coll,
            String proposedValidatorJson,
            Level level,
            Action action
    ) {
        public Rollout {
            Objects.requireNonNull(db, "db");
            Objects.requireNonNull(coll, "coll");
            Objects.requireNonNull(proposedValidatorJson, "proposedValidatorJson");
            Objects.requireNonNull(level, "level");
            Objects.requireNonNull(action, "action");
            if (proposedValidatorJson.isBlank())
                throw new IllegalArgumentException(
                        "validator JSON blank — use Level.OFF to disable");
        }
    }

    /** Preview verdict: how many of N sampled docs would fail, and
     *  the first few offenders. */
    public record PreviewResult(
            int sampled,
            int failedCount,
            java.util.List<FailedDoc> firstFew
    ) {
        public PreviewResult {
            firstFew = java.util.List.copyOf(firstFew);
        }
    }

    public record FailedDoc(String id, String summary) {}

    private ValidatorSpec() {}
}
