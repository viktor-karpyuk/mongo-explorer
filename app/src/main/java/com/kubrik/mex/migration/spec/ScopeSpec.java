package com.kubrik.mex.migration.spec;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

/** What gets migrated (SCOPE-1, SCOPE-2, SCOPE-9, SCOPE-10, SCOPE-11, SCOPE-12). Sealed:
 *  three granularities. Rename table is shared so all three can rename namespaces in transit.
 *  `include` / `exclude` follow the glob rules in docs/mvp-technical-spec.md §23.3.
 *
 *  <p>v1.2.0 introduces {@link ScopeFlags} (migrateIndexes + migrateUsers) and replaces the
 *  single-DB {@code Database} variant with a multi-DB {@link Databases} variant. Legacy
 *  profiles written before v1.2.0 are rewritten on load via {@code ProfileCodec}. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "mode")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ScopeSpec.Server.class,       name = "SERVER"),
        @JsonSubTypes.Type(value = ScopeSpec.Databases.class,    name = "DATABASES"),
        @JsonSubTypes.Type(value = ScopeSpec.Collections.class,  name = "COLLECTIONS")
})
public sealed interface ScopeSpec {

    ScopeFlags flags();
    List<String> include();
    List<String> exclude();
    List<Rename> renames();

    default boolean migrateIndexes() { return flags().migrateIndexes(); }
    default boolean migrateUsers()   { return flags().migrateUsers(); }

    /** Migrate every non-system database on the source. */
    record Server(
            ScopeFlags flags,
            List<String> include,
            List<String> exclude,
            List<Rename> renames
    ) implements ScopeSpec {}

    /** Migrate all (filtered) collections from {@code N ≥ 1} source databases (SCOPE-10). */
    record Databases(
            List<String> databases,
            ScopeFlags flags,
            List<String> include,
            List<String> exclude,
            List<Rename> renames
    ) implements ScopeSpec {}

    /** Migrate an explicit list of namespaces across one or more databases (SCOPE-11).
     *  {@code include} / {@code exclude} still apply as a secondary filter. */
    record Collections(
            List<Namespace> namespaces,
            ScopeFlags flags,
            List<String> include,
            List<String> exclude,
            List<Rename> renames
    ) implements ScopeSpec {}

    /** Rename one namespace in transit: {@code from} → {@code to}, both in {@code db.coll}
     *  form (SCOPE-9). */
    record Rename(String from, String to) {}
}
