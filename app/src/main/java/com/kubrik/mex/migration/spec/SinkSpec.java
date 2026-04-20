package com.kubrik.mex.migration.spec;

/** A non-MongoDB destination for a migration job's output (EXT-2).
 *  <p>
 *  When {@link MigrationSpec.Options#sinks()} is non-empty, the engine writes each plan's
 *  documents through the matching sink instead of a {@link com.mongodb.client.MongoCollection}.
 *  {@code path} is a base directory; concrete sinks decide how to lay out files inside it
 *  (NDJSON: {@code <db>.<coll>.ndjson}).
 *  <p>
 *  For plugin-provided sinks (EXT-1), set {@code kind = PLUGIN} and {@code pluginName} to the
 *  factory's declared name; the engine resolves it through
 *  {@link com.kubrik.mex.migration.sink.PluginSinkRegistry}. */
public record SinkSpec(
        SinkKind kind,
        String path,
        String pluginName
) {

    /** Back-compat constructor for built-in sinks. {@code pluginName} defaults to {@code null}. */
    public SinkSpec(SinkKind kind, String path) {
        this(kind, path, null);
    }

    public enum SinkKind {
        /** One BSON relaxed-JSON document per line. */
        NDJSON,
        /** A single JSON array of documents: {@code [{...},{...}]}. */
        JSON_ARRAY,
        /** Flat CSV — top-level fields as columns; nested / array values serialised as JSON
         *  strings; RFC 4180 quoting. Column set is discovered from the first batch. */
        CSV,
        /** Raw BSON concatenation compatible with {@code mongorestore}. */
        BSON_DUMP,
        /** Plugin-provided sink. The engine resolves it through
         *  {@link com.kubrik.mex.migration.sink.PluginSinkRegistry#resolve(String)} keyed by
         *  {@link #pluginName()}. */
        PLUGIN
    }
}
