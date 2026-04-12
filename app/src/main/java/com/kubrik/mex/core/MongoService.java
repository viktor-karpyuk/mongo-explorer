package com.kubrik.mex.core;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.util.ArrayList;
import java.util.List;

/** Thin wrapper around MongoClient with helpers for browse + ops. */
public class MongoService implements AutoCloseable {

    public static final JsonWriterSettings JSON_RELAXED =
            JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).indent(true).build();

    private final MongoClient client;
    private final String serverVersion;

    public MongoService(String uri) {
        ConnectionString cs = new ConnectionString(uri);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(cs)
                .applyToSocketSettings(b -> b.connectTimeout(8000, java.util.concurrent.TimeUnit.MILLISECONDS))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(8000, java.util.concurrent.TimeUnit.MILLISECONDS))
                .build();
        this.client = MongoClients.create(settings);
        Document buildInfo = client.getDatabase("admin").runCommand(new Document("buildInfo", 1));
        this.serverVersion = String.valueOf(buildInfo.get("version"));
    }

    public String serverVersion() { return serverVersion; }

    public List<String> listDatabaseNames() {
        List<String> names = new ArrayList<>();
        client.listDatabaseNames().forEach(names::add);
        return names;
    }

    public List<String> listCollectionNames(String db) {
        List<String> names = new ArrayList<>();
        client.getDatabase(db).listCollectionNames().forEach(names::add);
        names.sort(String.CASE_INSENSITIVE_ORDER);
        return names;
    }

    public Document dbStats(String db) {
        return client.getDatabase(db).runCommand(new Document("dbStats", 1));
    }

    public Document collStats(String db, String coll) {
        return client.getDatabase(db).runCommand(new Document("collStats", coll));
    }

    public void dropDatabase(String db) { client.getDatabase(db).drop(); }
    public void dropCollection(String db, String coll) { client.getDatabase(db).getCollection(coll).drop(); }
    public void createCollection(String db, String coll) { client.getDatabase(db).createCollection(coll); }
    public void renameCollection(String db, String coll, String newName) {
        client.getDatabase(db).getCollection(coll).renameCollection(
                new com.mongodb.MongoNamespace(db, newName));
    }

    public Document runCommand(String db, String json) {
        return client.getDatabase(db).runCommand(BsonDocument.parse(json));
    }

    public List<Document> listIndexes(String db, String coll) {
        List<Document> out = new ArrayList<>();
        client.getDatabase(db).getCollection(coll).listIndexes().forEach(out::add);
        return out;
    }

    public void createIndex(String db, String coll, String keysJson, boolean unique) {
        com.mongodb.client.model.IndexOptions opts = new com.mongodb.client.model.IndexOptions().unique(unique);
        client.getDatabase(db).getCollection(coll).createIndex(BsonDocument.parse(keysJson), opts);
    }

    /** Full-featured index creation matching Studio 3T options. */
    public void createIndex(String db, String coll, String keysJson,
                            boolean unique, boolean sparse,
                            long expireAfterSeconds, String name,
                            String partialFilterJson, boolean background) {
        com.mongodb.client.model.IndexOptions opts = new com.mongodb.client.model.IndexOptions()
                .unique(unique)
                .sparse(sparse)
                .background(background);
        if (expireAfterSeconds > 0) opts.expireAfter(expireAfterSeconds, java.util.concurrent.TimeUnit.SECONDS);
        if (name != null && !name.isBlank()) opts.name(name);
        if (partialFilterJson != null && !partialFilterJson.isBlank()) {
            opts.partialFilterExpression(BsonDocument.parse(partialFilterJson));
        }
        client.getDatabase(db).getCollection(coll).createIndex(BsonDocument.parse(keysJson), opts);
    }

    public void dropIndex(String db, String coll, String name) {
        client.getDatabase(db).getCollection(coll).dropIndex(name);
    }

    /* ============ User management ============ */

    public List<Document> listUsers(String db) {
        Document res = client.getDatabase(db).runCommand(new Document("usersInfo", 1));
        @SuppressWarnings("unchecked")
        List<Document> users = (List<Document>) res.get("users");
        return users == null ? List.of() : users;
    }

    public void createUser(String db, String username, String password, List<Document> roles) {
        Document cmd = new Document("createUser", username)
                .append("pwd", password)
                .append("roles", roles);
        client.getDatabase(db).runCommand(cmd);
    }

    public void dropUser(String db, String username) {
        client.getDatabase(db).runCommand(new Document("dropUser", username));
    }

    public void updateUserPassword(String db, String username, String newPassword) {
        client.getDatabase(db).runCommand(
                new Document("updateUser", username).append("pwd", newPassword));
    }

    public void grantRoles(String db, String username, List<Document> roles) {
        client.getDatabase(db).runCommand(
                new Document("grantRolesToUser", username).append("roles", roles));
    }

    public void revokeRoles(String db, String username, List<Document> roles) {
        client.getDatabase(db).runCommand(
                new Document("revokeRolesFromUser", username).append("roles", roles));
    }

    public MongoDatabase database(String db) { return client.getDatabase(db); }
    public MongoCollection<Document> collection(String db, String coll) {
        return client.getDatabase(db).getCollection(coll);
    }

    @Override
    public void close() { client.close(); }
}
