package com.kubrik.mex.migration.versioned;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Map;

/** One operation inside a versioned migration script. Sealed over the curated MVP set
 *  (docs/mvp-technical-spec.md §7.3).
 *
 *  <p>Serialised shape: a JSON object with an {@code "op"} discriminator. E.g.:
 *  <pre>{ "op": "createIndex", "collection": "users", "keys": { "email": 1 } }</pre>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "op")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Op.CreateCollection.class,  name = "createCollection"),
        @JsonSubTypes.Type(value = Op.CreateIndex.class,       name = "createIndex"),
        @JsonSubTypes.Type(value = Op.DropIndex.class,         name = "dropIndex"),
        @JsonSubTypes.Type(value = Op.RenameCollection.class,  name = "renameCollection"),
        @JsonSubTypes.Type(value = Op.UpdateMany.class,        name = "updateMany"),
        @JsonSubTypes.Type(value = Op.RenameField.class,       name = "renameField"),
        @JsonSubTypes.Type(value = Op.DropField.class,         name = "dropField"),
        @JsonSubTypes.Type(value = Op.RunCommand.class,        name = "runCommand")
})
public sealed interface Op {

    record CreateCollection(String collection, Map<String, Object> options) implements Op {}

    record CreateIndex(String collection, Map<String, Object> keys,
                       Map<String, Object> options) implements Op {}

    record DropIndex(String collection, String name) implements Op {}

    record RenameCollection(String collection, String to, boolean dropTarget) implements Op {}

    record UpdateMany(String collection,
                      Map<String, Object> filter,
                      Map<String, Object> update) implements Op {}

    record RenameField(String collection, String from, String to) implements Op {}

    record DropField(String collection, String field) implements Op {}

    record RunCommand(Map<String, Object> command) implements Op {}
}
