package com.kubrik.mex.migration.engine;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

/** Tiny helper for round-tripping a single {@link BsonValue} through extended JSON.
 *  Used by the resume file to persist per-collection and per-partition {@code lastId}
 *  without hard-coding a concrete BSON type. */
public final class BsonJson {

    private static final JsonWriterSettings EXTENDED =
            JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

    private BsonJson() {}

    public static String toJson(BsonValue v) {
        if (v == null) return null;
        return new BsonDocument("v", v).toJson(EXTENDED);
    }

    public static BsonValue fromJson(String json) {
        if (json == null || json.isBlank()) return null;
        BsonDocument wrap = BsonDocument.parse(json);
        return wrap.get("v");
    }
}
