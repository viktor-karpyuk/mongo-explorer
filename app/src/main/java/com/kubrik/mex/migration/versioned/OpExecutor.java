package com.kubrik.mex.migration.versioned;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Dispatches {@link Op} records to MongoDB driver calls. Pattern-matched over the sealed
 *  hierarchy so the compiler enforces exhaustiveness. */
public final class OpExecutor {

    private static final Logger log = LoggerFactory.getLogger(OpExecutor.class);

    private final MongoDatabase db;

    public OpExecutor(MongoDatabase db) {
        this.db = db;
    }

    public void execute(List<Op> ops) {
        for (Op op : ops) execute(op);
    }

    public void execute(Op op) {
        log.debug("executing {}", op);
        switch (op) {
            case Op.CreateCollection c -> {
                CreateCollectionOptions opts = new CreateCollectionOptions();
                // options are opaque; we pass through capped size if present
                if (c.options() != null && c.options().get("capped") instanceof Boolean capped && capped) {
                    opts.capped(true);
                    Number size = (Number) c.options().get("size");
                    if (size != null) opts.sizeInBytes(size.longValue());
                }
                db.createCollection(c.collection(), opts);
            }
            case Op.CreateIndex c -> {
                MongoCollection<Document> coll = db.getCollection(c.collection());
                Document keys = new Document(c.keys());
                IndexOptions idx = buildIndexOptions(c.options());
                coll.createIndex(keys, idx);
            }
            case Op.DropIndex d -> db.getCollection(d.collection()).dropIndex(d.name());
            case Op.RenameCollection r -> {
                MongoCollection<Document> coll = db.getCollection(r.collection());
                MongoNamespace ns = new MongoNamespace(db.getName(), r.to());
                coll.renameCollection(ns, new RenameCollectionOptions().dropTarget(r.dropTarget()));
            }
            case Op.UpdateMany u -> {
                Document filter = new Document(u.filter() == null ? Map.of() : u.filter());
                Document update = new Document(u.update());
                db.getCollection(u.collection()).updateMany(filter, update, new UpdateOptions());
            }
            case Op.RenameField r -> {
                Document rename = new Document(r.from(), r.to());
                db.getCollection(r.collection()).updateMany(Filters.empty(),
                        new Document("$rename", rename));
            }
            case Op.DropField d -> {
                Document unset = new Document(d.field(), "");
                db.getCollection(d.collection()).updateMany(Filters.empty(),
                        new Document("$unset", unset));
            }
            case Op.RunCommand r -> db.runCommand(new Document(r.command()));
        }
    }

    private static IndexOptions buildIndexOptions(Map<String, Object> options) {
        IndexOptions o = new IndexOptions();
        if (options == null) return o;
        if (options.get("unique") instanceof Boolean u) o.unique(u);
        if (options.get("sparse") instanceof Boolean s) o.sparse(s);
        if (options.get("background") instanceof Boolean bg) o.background(bg);
        if (options.get("name") instanceof String n) o.name(n);
        if (options.get("expireAfterSeconds") instanceof Number ttl) {
            o.expireAfter(ttl.longValue(), TimeUnit.SECONDS);
        }
        if (options.get("partialFilterExpression") instanceof Map<?, ?> pfe) {
            @SuppressWarnings("unchecked")
            Document d = new Document((Map<String, Object>) pfe);
            o.partialFilterExpression(d);
        }
        return o;
    }
}
