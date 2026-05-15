package com.kubrik.mex.shell;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyExecutable;
import org.graalvm.polyglot.proxy.ProxyObject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JS-side facade for a MongoDB database. Member access by name is the
 * mongosh shorthand for "give me a collection": {@code
 * db.masterUserProfiles} resolves to a {@link CollectionProxy} for
 * "masterUserProfiles". Reserved methods ({@code getCollection},
 * {@code runCommand}, {@code getCollectionNames}, etc.) take
 * precedence — type {@code db.getCollection("…")} when the collection
 * name shadows a method.
 */
final class DatabaseProxy implements ProxyObject {

    private final MongoClient client;
    private volatile String currentDbName;
    private volatile MongoDatabase currentDb;

    /** Per-name cache so {@code db.users} repeated in a script returns
     *  the same proxy and the same underlying {@link com.mongodb.client.MongoCollection}. */
    private final Map<String, CollectionProxy> collCache = new ConcurrentHashMap<>();
    private final Map<String, Object> methods = new LinkedHashMap<>();

    DatabaseProxy(MongoClient client, String defaultDbName) {
        this.client = client;
        switchTo(defaultDbName);
        bindMethods();
    }

    void switchTo(String dbName) {
        this.currentDbName = dbName == null ? "test" : dbName;
        this.currentDb = client.getDatabase(this.currentDbName);
        this.collCache.clear();
    }

    String currentDbName() { return currentDbName; }

    private void bindMethods() {
        methods.put("getName",           (ProxyExecutable) a -> currentDbName);
        methods.put("getCollection",     (ProxyExecutable) a -> coll(a[0].asString()));
        methods.put("getCollectionNames",(ProxyExecutable) a -> {
            List<String> names = new ArrayList<>();
            currentDb.listCollectionNames().forEach(names::add);
            return names;
        });
        methods.put("listCollections",   (ProxyExecutable) a -> {
            List<Document> out = new ArrayList<>();
            currentDb.listCollections().forEach(out::add);
            return out;
        });
        methods.put("createCollection",  (ProxyExecutable) a -> {
            currentDb.createCollection(a[0].asString());
            return "ok";
        });
        methods.put("dropDatabase",      (ProxyExecutable) a -> { currentDb.drop(); return "ok"; });
        methods.put("runCommand",        (ProxyExecutable) a -> {
            Document cmd = JsBsonConverter.toDocument(a[0]);
            return currentDb.runCommand(cmd);
        });
        methods.put("adminCommand",      (ProxyExecutable) a -> {
            Document cmd = JsBsonConverter.toDocument(a[0]);
            return client.getDatabase("admin").runCommand(cmd);
        });
        methods.put("stats",             (ProxyExecutable) a -> currentDb.runCommand(new Document("dbStats", 1)));
        methods.put("getMongo",          (ProxyExecutable) a -> "mongo-explorer-shell");
    }

    private CollectionProxy coll(String name) {
        return collCache.computeIfAbsent(name, n -> new CollectionProxy(currentDb, currentDbName, n));
    }

    @Override public Object getMember(String key) {
        // Reserved methods first so users can't accidentally shadow them
        // by naming a collection "runCommand" (use db.getCollection("…")).
        Object m = methods.get(key);
        if (m != null) return m;
        return coll(key);
    }

    @Override public Object getMemberKeys() {
        // Method names + cached collection names. Listing every
        // collection on demand (live driver call) for completeness
        // would block the FX thread on large clusters; the cached
        // names already cover anything the user has touched in this
        // shell session.
        List<String> keys = new ArrayList<>(methods.keySet());
        keys.addAll(collCache.keySet());
        return keys.toArray(new String[0]);
    }

    @Override public boolean hasMember(String key) {
        return methods.containsKey(key) || true; // any name is a valid collection
    }

    @Override public void putMember(String key, Value value) {
        throw new UnsupportedOperationException("db members are read-only — use getCollection(name) for dynamic access");
    }
}
