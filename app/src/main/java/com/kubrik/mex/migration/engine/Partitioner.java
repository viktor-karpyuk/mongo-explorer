package com.kubrik.mex.migration.engine;

import com.kubrik.mex.migration.spec.PerfSpec;
import com.mongodb.MongoCommandException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Sorts;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Splits a large collection into {@code _id} ranges for parallel reads (PERF-3).
 *  <p>
 *  Tries {@code splitVector} first (replica-set / standalone, admin-ish privilege); falls
 *  back to {@code $sample} when the server rejects the command (typical on Atlas M0/M2). */
public final class Partitioner {

    private static final Logger log = LoggerFactory.getLogger(Partitioner.class);

    private Partitioner() {}

    public static List<Partition> split(MongoDatabase db,
                                        MongoCollection<RawBsonDocument> coll,
                                        PerfSpec perf) {
        long count = ApproxCount.of(db, coll);
        if (count < perf.partitionThreshold()) return List.of(Partition.FULL);

        try {
            return viaSplitVector(db, coll, perf);
        } catch (MongoCommandException e) {
            log.info("splitVector unavailable ({}); falling back to $sample partitioning", e.getErrorCodeName());
            return viaSampleRanges(coll, desiredPartitions(count, perf));
        } catch (Exception e) {
            log.warn("splitVector failed ({}); falling back to $sample partitioning", e.toString());
            return viaSampleRanges(coll, desiredPartitions(count, perf));
        }
    }

    private static List<Partition> viaSplitVector(MongoDatabase db,
                                                  MongoCollection<RawBsonDocument> coll,
                                                  PerfSpec perf) {
        String fullName = coll.getNamespace().getFullName();
        Document res = db.runCommand(new Document("splitVector", fullName)
                .append("keyPattern", new Document("_id", 1))
                .append("maxChunkSizeBytes", 64L * 1024 * 1024));
        @SuppressWarnings("unchecked")
        List<Document> keys = (List<Document>) res.get("splitKeys");
        if (keys == null || keys.isEmpty()) return List.of(Partition.FULL);

        List<BsonValue> ids = new ArrayList<>(keys.size());
        for (Document d : keys) {
            // Round-trip through BsonDocument to get proper BsonValue typing.
            BsonDocument b = BsonDocument.parse(d.toJson());
            ids.add(b.get("_id"));
        }
        return rangesFrom(ids);
    }

    private static List<Partition> viaSampleRanges(MongoCollection<RawBsonDocument> coll, int desired) {
        if (desired <= 1) return List.of(Partition.FULL);
        int sampleSize = Math.max(desired - 1, 1);
        AggregateIterable<Document> it = coll
                .withDocumentClass(Document.class)
                .aggregate(List.of(
                        Aggregates.sample(sampleSize),
                        Aggregates.project(new Document("_id", 1)),
                        Aggregates.sort(Sorts.ascending("_id"))));
        List<BsonValue> ids = new ArrayList<>(sampleSize);
        for (Document d : it) {
            BsonDocument b = BsonDocument.parse(d.toJson());
            ids.add(b.get("_id"));
        }
        ids.sort(Comparator.comparing(BsonValue::toString));
        return rangesFrom(ids);
    }

    private static List<Partition> rangesFrom(List<BsonValue> splitKeys) {
        List<Partition> out = new ArrayList<>(splitKeys.size() + 1);
        BsonValue prev = null;
        for (int i = 0; i < splitKeys.size(); i++) {
            BsonValue k = splitKeys.get(i);
            out.add(new Partition(prev, k, i));
            prev = k;
        }
        out.add(new Partition(prev, null, splitKeys.size()));
        return out;
    }

    private static int desiredPartitions(long count, PerfSpec perf) {
        long chunks = Math.max(1, count / perf.partitionThreshold());
        return (int) Math.min(16, chunks);
    }

    /** Exposed for tests. */
    static Bson rangeFilter(Partition p) { return p.toFilter(); }
}
