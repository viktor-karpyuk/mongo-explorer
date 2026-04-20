package com.kubrik.mex.migration.sink;

import com.kubrik.mex.migration.spec.SinkSpec;

/** External SPI for plugin-provided sink kinds (EXT-1).
 *  <p>
 *  Implementations are discovered at app startup via {@link java.util.ServiceLoader} from
 *  JAR files dropped into {@link com.kubrik.mex.store.AppPaths#pluginsDir()}. Each JAR
 *  declares its factory class in
 *  {@code META-INF/services/com.kubrik.mex.migration.sink.MigrationSinkFactory}.
 *  <p>
 *  A user references a plugin in a profile YAML by setting {@code kind: PLUGIN} and
 *  {@code pluginName: <factory-name>}; the engine looks up the factory by its declared
 *  {@link #name()} and calls {@link #create(SinkSpec)} to obtain a live sink.
 *  <p>
 *  <b>Status:</b> {@code @Beta} for the first release — forward-compatible additions are
 *  allowed but removals / signature changes will be called out in release notes. */
public interface MigrationSinkFactory {

    /** Stable identifier for this factory. Must match the {@code pluginName} a user writes in
     *  {@link SinkSpec#pluginName()}. Case-sensitive. Factory authors should pick a short,
     *  unique name (e.g. {@code "parquet"}, {@code "orc"}, {@code "s3-ndjson"}). */
    String name();

    /** Human-readable extension (with leading dot — e.g. {@code ".parquet"}) used by preflight
     *  to warn about files that would be overwritten. Returning an empty string disables the
     *  overwrite warning. */
    default String extension() { return ""; }

    /** Build a fresh sink instance for a single target namespace. {@link SinkSpec#path()} is
     *  the user-supplied base directory; it is the factory's responsibility to lay out files
     *  inside it as it sees fit. Implementations must not retain references to {@code spec}
     *  beyond the call. */
    MigrationSink create(SinkSpec spec);
}
