package com.kubrik.mex.maint.schema;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * v2.7 SCHV-8 — Starter templates for the schema validator editor.
 * Users pick one and edit; the editor never auto-authors a schema
 * (that's explicitly out-of-scope per NG-2.7-5).
 */
public final class StarterTemplates {

    public record Template(String name, String description, String json) {}

    private static final Map<String, Template> BY_NAME = new LinkedHashMap<>();

    static {
        register(new Template("empty",
                "An empty $jsonSchema — every doc passes. Starting point "
                        + "for a slow rollout.",
                """
                { "$jsonSchema": { "bsonType": "object" } }
                """.strip()));

        register(new Template("doc-with-required-id",
                "Requires an _id of ObjectId type. Minimum safe baseline "
                        + "for application collections.",
                """
                {
                  "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["_id"],
                    "properties": {
                      "_id": { "bsonType": "objectId" }
                    }
                  }
                }
                """.strip()));

        register(new Template("doc-with-enum-status",
                "Common shape: required status field constrained to a "
                        + "fixed enum. Good starting point for domain docs.",
                """
                {
                  "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["status"],
                    "properties": {
                      "status": {
                        "enum": ["PENDING", "ACTIVE", "PAUSED", "ARCHIVED"],
                        "description": "Lifecycle state."
                      }
                    }
                  }
                }
                """.strip()));

        register(new Template("doc-with-typed-timestamps",
                "Enforces createdAt / updatedAt as BSON Date. Catches "
                        + "the common ISO-string-instead-of-Date bug.",
                """
                {
                  "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["createdAt", "updatedAt"],
                    "properties": {
                      "createdAt": { "bsonType": "date" },
                      "updatedAt": { "bsonType": "date" }
                    }
                  }
                }
                """.strip()));
    }

    private static void register(Template t) {
        BY_NAME.put(t.name(), t);
    }

    public static java.util.Collection<Template> all() {
        return BY_NAME.values();
    }

    public static Template byName(String name) {
        Template t = BY_NAME.get(name);
        if (t == null) throw new IllegalArgumentException(
                "unknown template: " + name);
        return t;
    }

    private StarterTemplates() {}
}
