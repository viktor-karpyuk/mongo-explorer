package com.kubrik.mex.shell;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyExecutable;
import org.graalvm.polyglot.proxy.ProxyObject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * JS-side facade for a MongoDB collection. Exposes the standard
 * mongosh methods as members so {@code db.users.updateOne(...)}
 * resolves through {@link ProxyObject#getMember} to the right
 * executable.
 *
 * <p>Returned values are either Java/BSON (which Graal exposes to JS
 * automatically — {@code result.modifiedCount} works) or {@link
 * CursorProxy} for the methods that return a cursor in mongosh.
 */
final class CollectionProxy implements ProxyObject {

    private final MongoDatabase db;
    private final MongoCollection<Document> coll;
    private final String dbName;
    private final String collName;

    private final Map<String, Object> members = new LinkedHashMap<>();

    CollectionProxy(MongoDatabase db, String dbName, String collName) {
        this.db = db;
        this.dbName = dbName;
        this.collName = collName;
        this.coll = db.getCollection(collName);
        bind();
    }

    private void bind() {
        members.put("getName", (ProxyExecutable) a -> collName);
        members.put("getFullName", (ProxyExecutable) a -> dbName + "." + collName);

        // ---- find / findOne ----
        members.put("find", (ProxyExecutable) a -> {
            Document filter = a.length > 0 ? JsBsonConverter.toDocument(a[0]) : new Document();
            Document projection = a.length > 1 ? JsBsonConverter.toDocument(a[1]) : null;
            FindIterable<Document> it = coll.find(filter);
            if (projection != null && !projection.isEmpty()) it.projection(projection);
            return new CursorProxy(it);
        });
        members.put("findOne", (ProxyExecutable) a -> {
            Document filter = a.length > 0 ? JsBsonConverter.toDocument(a[0]) : new Document();
            Document projection = a.length > 1 ? JsBsonConverter.toDocument(a[1]) : null;
            FindIterable<Document> it = coll.find(filter);
            if (projection != null && !projection.isEmpty()) it.projection(projection);
            return it.first();
        });
        members.put("countDocuments", (ProxyExecutable) a -> {
            Document filter = a.length > 0 ? JsBsonConverter.toDocument(a[0]) : new Document();
            return coll.countDocuments(filter);
        });
        members.put("estimatedDocumentCount", (ProxyExecutable) a -> coll.estimatedDocumentCount());
        members.put("distinct", (ProxyExecutable) a -> {
            String field = a[0].asString();
            Document filter = a.length > 1 ? JsBsonConverter.toDocument(a[1]) : new Document();
            List<Object> out = new ArrayList<>();
            coll.distinct(field, filter, Object.class).forEach(out::add);
            return out;
        });

        // ---- insert ----
        members.put("insertOne", (ProxyExecutable) a -> {
            Document d = JsBsonConverter.toDocument(a[0]);
            InsertOneResult r = coll.insertOne(d);
            Document out = new Document();
            out.put("acknowledged", r.wasAcknowledged());
            out.put("insertedId", r.getInsertedId() == null ? null : r.getInsertedId().toString().contains("=")
                    ? r.getInsertedId() : r.getInsertedId());
            return out;
        });
        members.put("insertMany", (ProxyExecutable) a -> {
            List<Document> docs = JsBsonConverter.toDocumentList(a[0]);
            boolean ordered = true;
            if (a.length > 1) {
                Document opts = JsBsonConverter.toDocument(a[1]);
                if (opts.get("ordered") instanceof Boolean b) ordered = b;
            }
            InsertManyResult r = coll.insertMany(docs, new InsertManyOptions().ordered(ordered));
            Document out = new Document();
            out.put("acknowledged", r.wasAcknowledged());
            out.put("insertedIds", r.getInsertedIds());
            return out;
        });

        // ---- update / replace ----
        members.put("updateOne", (ProxyExecutable) a -> {
            Document filter = JsBsonConverter.toDocument(a[0]);
            Document update = JsBsonConverter.toDocument(a[1]);
            UpdateOptions opts = parseUpdateOptions(a.length > 2 ? a[2] : null);
            UpdateResult r = coll.updateOne(filter, update, opts);
            return updateResultToDocument(r);
        });
        members.put("updateMany", (ProxyExecutable) a -> {
            Document filter = JsBsonConverter.toDocument(a[0]);
            Document update = JsBsonConverter.toDocument(a[1]);
            UpdateOptions opts = parseUpdateOptions(a.length > 2 ? a[2] : null);
            UpdateResult r = coll.updateMany(filter, update, opts);
            return updateResultToDocument(r);
        });
        members.put("replaceOne", (ProxyExecutable) a -> {
            Document filter      = JsBsonConverter.toDocument(a[0]);
            Document replacement = JsBsonConverter.toDocument(a[1]);
            ReplaceOptions opts = new ReplaceOptions();
            if (a.length > 2) {
                Document o = JsBsonConverter.toDocument(a[2]);
                if (o.get("upsert") instanceof Boolean u) opts.upsert(u);
            }
            UpdateResult r = coll.replaceOne(filter, replacement, opts);
            return updateResultToDocument(r);
        });

        // ---- delete ----
        members.put("deleteOne", (ProxyExecutable) a -> {
            Document filter = JsBsonConverter.toDocument(a[0]);
            DeleteResult r = coll.deleteOne(filter, new DeleteOptions());
            Document out = new Document();
            out.put("acknowledged", r.wasAcknowledged());
            out.put("deletedCount", r.getDeletedCount());
            return out;
        });
        members.put("deleteMany", (ProxyExecutable) a -> {
            Document filter = JsBsonConverter.toDocument(a[0]);
            DeleteResult r = coll.deleteMany(filter, new DeleteOptions());
            Document out = new Document();
            out.put("acknowledged", r.wasAcknowledged());
            out.put("deletedCount", r.getDeletedCount());
            return out;
        });

        // ---- aggregate ----
        members.put("aggregate", (ProxyExecutable) a -> {
            List<Document> pipeline = JsBsonConverter.toDocumentList(a[0]);
            AggregateIterable<Document> it = coll.aggregate(pipeline);
            return new CursorProxy(it);
        });

        // ---- index ops ----
        members.put("createIndex", (ProxyExecutable) a -> {
            Document keys = JsBsonConverter.toDocument(a[0]);
            IndexOptions opts = new IndexOptions();
            if (a.length > 1) {
                Document o = JsBsonConverter.toDocument(a[1]);
                if (o.get("unique") instanceof Boolean u) opts.unique(u);
                if (o.get("sparse") instanceof Boolean s) opts.sparse(s);
                if (o.get("name")   instanceof String n)  opts.name(n);
                if (o.get("expireAfterSeconds") instanceof Number n) {
                    opts.expireAfter(n.longValue(), java.util.concurrent.TimeUnit.SECONDS);
                }
                if (o.get("background") instanceof Boolean b) opts.background(b);
                if (o.get("partialFilterExpression") instanceof Document pfe) {
                    opts.partialFilterExpression(pfe);
                }
            }
            return coll.createIndex(keys, opts);
        });
        members.put("getIndexes", (ProxyExecutable) a -> {
            List<Document> out = new ArrayList<>();
            coll.listIndexes().forEach(out::add);
            return out;
        });
        members.put("dropIndex", (ProxyExecutable) a -> {
            String name = a[0].asString();
            coll.dropIndex(name);
            return "ok";
        });

        // ---- collection ops ----
        members.put("drop", (ProxyExecutable) a -> { coll.drop(); return "ok"; });
        members.put("renameCollection", (ProxyExecutable) a -> {
            String newName = a[0].asString();
            coll.renameCollection(new com.mongodb.MongoNamespace(dbName, newName));
            return "ok";
        });
        members.put("stats", (ProxyExecutable) a -> db.runCommand(new Document("collStats", collName)));

        // ---- bulkWrite ----
        members.put("bulkWrite", (ProxyExecutable) a -> BulkWriteAdapter.run(coll, a[0]));
    }

    private static UpdateOptions parseUpdateOptions(Value optsArg) {
        UpdateOptions opts = new UpdateOptions();
        if (optsArg == null || optsArg.isNull()) return opts;
        Document o = JsBsonConverter.toDocument(optsArg);
        if (o.get("upsert") instanceof Boolean u) opts.upsert(u);
        return opts;
    }

    private static Document updateResultToDocument(UpdateResult r) {
        Document out = new Document();
        out.put("acknowledged", r.wasAcknowledged());
        out.put("matchedCount", r.getMatchedCount());
        out.put("modifiedCount", r.getModifiedCount());
        out.put("upsertedId", r.getUpsertedId());
        return out;
    }

    @Override public Object getMember(String key) {
        return members.get(key);
    }

    @Override public Object getMemberKeys() {
        return members.keySet().toArray(new String[0]);
    }

    @Override public boolean hasMember(String key) {
        return members.containsKey(key);
    }

    @Override public void putMember(String key, Value value) {
        throw new UnsupportedOperationException("collection members are read-only");
    }

    Set<String> memberKeys() { return members.keySet(); }
}
