package com.kubrik.mex.migration.engine;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/** Estimates a collection's document count cheaply. Prefers {@code collStats.count}
 *  (O(1), possibly stale by a few seconds); falls back to {@code countDocuments()}
 *  only when stats are unavailable. Preflight never blocks on the slow path.
 *  <p>
 *  See docs/mvp-technical-spec.md §6.5 and §20.3. */
public final class ApproxCount {

    private ApproxCount() {}

    public static long of(MongoDatabase db, MongoCollection<?> coll) {
        try {
            Document stats = db.runCommand(new Document("collStats", coll.getNamespace().getCollectionName()));
            Number n = (Number) stats.get("count");
            if (n != null) return n.longValue();
        } catch (Exception ignored) {
            // stats unavailable — fall through
        }
        return coll.countDocuments();
    }
}
