package com.kubrik.mex.migration.spec;

import java.util.List;
import java.util.Map;

/** Immutable, canonical description of a single migration job.
 *  <p>
 *  The facade ({@link com.kubrik.mex.migration.MigrationService}) takes a spec, runs preflight
 *  against it, and either saves it as a profile or starts a job. Everything about the job's
 *  behaviour is captured here; once a job is running the spec is frozen (BR-4).
 *  <p>
 *  Serialisation is handled by {@code ProfileCodec}; the canonical form (schema v1) is
 *  documented in docs/mvp-technical-spec.md §5.1.
 */
public record MigrationSpec(
        int schema,                                  // always 1 for this spec version
        MigrationKind kind,
        String name,                                 // optional human label
        SourceSpec source,
        TargetSpec target,
        ScopeSpec scope,                             // null for VERSIONED
        String scriptsFolder,                        // non-null for VERSIONED
        Options options
) {

    /** Tuning + transforms + execution mode grouped so they can evolve without changing the
     *  top-level spec shape (profile schema v2 / v3 forward-compat). */
    public record Options(
            ExecutionMode executionMode,
            Conflict conflict,
            Map<String, TransformSpec> transform,   // keyed by source namespace (db.coll)
            PerfSpec performance,
            VerifySpec verification,
            ErrorPolicy errorPolicy,
            boolean ignoreDrift,                    // VER-4 — acknowledge checksum drift and proceed
            String environment,                     // VER-8 — gates scripts tagged with `env` (null = unrestricted)
            List<SinkSpec> sinks                    // EXT-2 — non-MongoDB destinations; empty = write to target collection
    ) {
        /** Canonical constructor — treats a null sinks list as empty so every existing call
         *  site that used the 8-arg form via Jackson / tests keeps working. */
        public Options {
            sinks = sinks == null ? List.of() : List.copyOf(sinks);
        }

        public static Options defaults() {
            return new Options(
                    ExecutionMode.RUN,
                    Conflict.defaults(),
                    Map.of(),
                    PerfSpec.defaults(),
                    VerifySpec.defaults(),
                    ErrorPolicy.defaults(),
                    false,
                    null,
                    List.of());
        }
    }

    /** Default conflict mode plus per-collection overrides. */
    public record Conflict(
            ConflictMode defaultMode,
            Map<String, ConflictMode> perCollection
    ) {
        public static Conflict defaults() {
            return new Conflict(ConflictMode.ABORT, Map.of());
        }

        public ConflictMode modeFor(String namespace) {
            return perCollection.getOrDefault(namespace, defaultMode);
        }
    }
}
