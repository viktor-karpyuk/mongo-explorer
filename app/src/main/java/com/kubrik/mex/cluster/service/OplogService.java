package com.kubrik.mex.cluster.service;

import com.kubrik.mex.cluster.ops.OplogEntry;
import com.kubrik.mex.cluster.ops.OplogGaugeStats;
import com.kubrik.mex.core.MongoService;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

/**
 * v2.4 OPLOG-1..9 — one-shot probes of the {@code local.oplog.rs} capped
 * collection. Returns {@link OplogGaugeStats#unsupported} + an empty tail on
 * standalone servers or when permissions block the read; callers surface
 * that as the "oplog unavailable" banner rather than an error.
 */
public final class OplogService {

    private static final int MAX_TIME_MS = 3_000;

    private OplogService() {}

    public static OplogGaugeStats sampleGauge(MongoService svc) {
        if (svc == null) return OplogGaugeStats.unsupported("not_connected");
        try {
            MongoDatabase local = svc.database("local");
            Document stats = local.runCommand(new Document("collStats", "oplog.rs")
                    .append("maxTimeMS", MAX_TIME_MS));
            long size = longValue(stats.get("maxSize"));
            if (size == 0) size = longValue(stats.get("storageSize"));
            long used = longValue(stats.get("size"));

            MongoCollection<Document> oplog = local.getCollection("oplog.rs");
            long firstSec = 0L;
            long lastSec = 0L;
            Document first = oplog.find().sort(new Document("$natural", 1))
                    .projection(new Document("ts", 1))
                    .limit(1).maxTime(MAX_TIME_MS, java.util.concurrent.TimeUnit.MILLISECONDS)
                    .first();
            if (first != null && first.get("ts") instanceof BsonTimestamp bt) firstSec = bt.getTime();
            Document last = oplog.find().sort(new Document("$natural", -1))
                    .projection(new Document("ts", 1))
                    .limit(1).maxTime(MAX_TIME_MS, java.util.concurrent.TimeUnit.MILLISECONDS)
                    .first();
            if (last != null && last.get("ts") instanceof BsonTimestamp bt) lastSec = bt.getTime();
            double window = (firstSec == 0 || lastSec == 0) ? 0.0 : (lastSec - firstSec) / 3600.0;
            return new OplogGaugeStats(true, size, used, firstSec, lastSec, window, null);
        } catch (Exception e) {
            return OplogGaugeStats.unsupported(e.getMessage());
        }
    }

    /**
     * Tails the last {@code limit} entries optionally filtered by namespace
     * regex + op type. Filtering happens server-side so the driver only pulls
     * matching documents. Results are newest-first.
     */
    public static List<OplogEntry> tail(MongoService svc, int limit,
                                        String nsRegex, List<String> opTypes) {
        if (svc == null || limit <= 0) return List.of();
        List<Bson> filters = new ArrayList<>();
        if (nsRegex != null && !nsRegex.isBlank()) {
            filters.add(new Document("ns", new Document("$regex", nsRegex)));
        }
        if (opTypes != null && !opTypes.isEmpty()) {
            filters.add(new Document("op", new Document("$in", opTypes)));
        }
        Bson filter = filters.isEmpty() ? new Document() : combine(filters);
        try {
            MongoCollection<Document> oplog = svc.database("local").getCollection("oplog.rs");
            FindIterable<Document> it = oplog.find(filter)
                    .sort(new Document("$natural", -1))
                    .limit(Math.min(limit, 2_000))
                    .maxTime(MAX_TIME_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
            List<OplogEntry> out = new ArrayList<>();
            for (Document d : it) out.add(OplogEntry.fromRaw(d));
            return out;
        } catch (Exception e) {
            return List.of();
        }
    }

    private static Bson combine(List<Bson> clauses) {
        return new Document("$and", clauses);
    }

    private static long longValue(Object o) {
        return o instanceof Number n ? n.longValue() : 0L;
    }
}
