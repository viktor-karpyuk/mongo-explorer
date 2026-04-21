package com.kubrik.mex.core;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.util.ArrayList;
import java.util.List;

/** Thin wrapper around MongoClient with helpers for browse + ops. */
public class MongoService implements AutoCloseable {

    public static final JsonWriterSettings JSON_RELAXED =
            JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).indent(true).build();

    private final MongoClient client;
    private final ConnectionString cs;
    private final String serverVersion;
    private volatile Document cachedHello;

    public MongoService(String uri) {
        this.cs = new ConnectionString(uri);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(cs)
                .applyToSocketSettings(b -> {
                    b.connectTimeout(8000, java.util.concurrent.TimeUnit.MILLISECONDS);
                    b.readTimeout(30000, java.util.concurrent.TimeUnit.MILLISECONDS);
                })
                .applyToClusterSettings(b -> b.serverSelectionTimeout(8000, java.util.concurrent.TimeUnit.MILLISECONDS))
                .applyToConnectionPoolSettings(b -> b.maxWaitTime(10000, java.util.concurrent.TimeUnit.MILLISECONDS))
                .build();
        this.client = MongoClients.create(settings);
        Document buildInfo = client.getDatabase("admin").runCommand(new Document("buildInfo", 1));
        this.serverVersion = String.valueOf(buildInfo.get("version"));
    }

    /**
     * v2.4 Q2.4-A follow-up — opens a short-lived client against a direct peer
     * (e.g., one shard's replica-set seeds) reusing this service's credentials
     * and TLS settings. {@code rsHostSpec} is the {@code rsName/h1:p,h2:p,...}
     * format returned by {@code listShards.host}; only the host list after the
     * slash is used by the driver, but the replica-set name is set on the
     * settings so hello routes straight to the primary.
     *
     * <p>Caller owns the returned client and must close it. Timeouts are tight
     * (caller-controlled) so a down shard doesn't stall the topology tick.</p>
     */
    public MongoClient openPeerClient(String rsHostSpec, int timeoutMs) {
        if (rsHostSpec == null || rsHostSpec.isBlank()) {
            throw new IllegalArgumentException("rsHostSpec");
        }
        String rsName = null;
        String hostList = rsHostSpec;
        int slash = rsHostSpec.indexOf('/');
        if (slash > 0) {
            rsName = rsHostSpec.substring(0, slash);
            hostList = rsHostSpec.substring(slash + 1);
        }
        java.util.List<com.mongodb.ServerAddress> addrs = new java.util.ArrayList<>();
        for (String h : hostList.split(",")) {
            String trimmed = h.trim();
            if (trimmed.isEmpty()) continue;
            int colon = trimmed.lastIndexOf(':');
            if (colon > 0) {
                addrs.add(new com.mongodb.ServerAddress(trimmed.substring(0, colon),
                        Integer.parseInt(trimmed.substring(colon + 1))));
            } else {
                addrs.add(new com.mongodb.ServerAddress(trimmed));
            }
        }
        if (addrs.isEmpty()) throw new IllegalArgumentException("no hosts in " + rsHostSpec);

        String replicaSetName = rsName;
        MongoClientSettings.Builder b = MongoClientSettings.builder()
                .applyToClusterSettings(c -> {
                    c.hosts(addrs);
                    if (replicaSetName != null) c.requiredReplicaSetName(replicaSetName);
                    c.serverSelectionTimeout(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                })
                .applyToSocketSettings(c -> {
                    c.connectTimeout(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                    c.readTimeout(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                })
                .applyToSslSettings(c -> c.enabled(Boolean.TRUE.equals(cs.getSslEnabled())))
                .applyToConnectionPoolSettings(c -> c.maxSize(2));
        if (cs.getCredential() != null) b.credential(cs.getCredential());
        return MongoClients.create(b.build());
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

    /** Full {@code listCollections} entry for a single namespace — used by SCOPE-5 (options)
     *  and SCOPE-6 (views) which need both the {@code options} sub-document and the
     *  {@code type} field ({@code "collection"} vs {@code "view"}). Returns an empty Document
     *  when the collection doesn't exist. */
    public Document collectionInfo(String db, String coll) {
        for (Document d : client.getDatabase(db).listCollections()
                .filter(new Document("name", coll))) {
            return d;
        }
        return new Document();
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

    /** RawBson view of a collection — used by the migration engine to preserve BSON fidelity
     *  (see docs/mvp-technical-spec.md §15.2). */
    public MongoCollection<RawBsonDocument> rawCollection(String db, String coll) {
        return client.getDatabase(db).getCollection(coll, RawBsonDocument.class);
    }

    /** Cached `hello` output — used by preflight to determine topology and version without
     *  hammering the cluster when the wizard navigates between steps. */
    public Document hello() {
        Document h = cachedHello;
        if (h != null) return h;
        synchronized (this) {
            if (cachedHello == null) {
                cachedHello = client.getDatabase("admin").runCommand(new Document("hello", 1));
            }
            return cachedHello;
        }
    }

    @Override
    public void close() { client.close(); }
}
