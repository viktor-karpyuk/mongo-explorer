package com.kubrik.mex.migration.engine;

import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.migration.spec.PerfSpec;
import com.kubrik.mex.migration.spec.TransformSpec;
import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/** Streams a source collection partition into a bounded batch queue.
 *  <p>
 *  Respects the configured read preference (SRC-7), applies the transform's filter and
 *  projection, keeps the cursor alive for long migrations ({@code noCursorTimeout(true)}),
 *  and stops promptly on {@code ctx.stopping()}.
 *  <p>
 *  See docs/mvp-technical-spec.md §6.2. */
public final class Reader {

    private final JobContext ctx;
    private final MongoCollection<RawBsonDocument> source;
    private final Partition partition;
    private final TransformSpec xform;
    private final BlockingQueue<Batch> queue;
    private final BsonValue resumeAfterId;

    public Reader(JobContext ctx,
                  MongoCollection<RawBsonDocument> source,
                  Partition partition,
                  TransformSpec xform,
                  BlockingQueue<Batch> queue,
                  BsonValue resumeAfterId) {
        this.ctx = ctx;
        this.source = source;
        this.partition = partition;
        this.xform = xform;
        this.queue = queue;
        this.resumeAfterId = resumeAfterId;
    }

    public void run() throws InterruptedException {
        MigrationSpec spec = ctx.spec();
        PerfSpec perf = spec.options().performance();
        ReadPreference rp = readPreference(spec.source().readPreference());
        MongoCollection<RawBsonDocument> pinned = source.withReadPreference(rp);

        try (MongoCursor<RawBsonDocument> cur = openCursor(pinned, perf)) {
            streamInto(cur, perf);
        }
        queue.put(Batch.POISON);
    }

    private MongoCursor<RawBsonDocument> openCursor(MongoCollection<RawBsonDocument> pinned, PerfSpec perf) {
        // XFORM-5 — source-side aggregation entirely replaces find/filter/projection.
        // Partitioning and resume-after are bypassed for this path: the user's pipeline
        // is responsible for its own ordering and completeness. SAFE-1 size estimates
        // become unreliable (documented in the milestone risk table).
        if (xform != null && xform.hasSourceAggregation()) {
            List<Bson> pipeline = parsePipeline(xform.sourceAggregationJson());
            return pinned.aggregate(pipeline)
                    .batchSize(perf.batchDocs())
                    .allowDiskUse(true)
                    .iterator();
        }

        FindIterable<RawBsonDocument> find = pinned.find(buildFilter())
                .sort(new Document("_id", 1))
                .batchSize(perf.batchDocs())
                .noCursorTimeout(true);

        if (xform != null && xform.projectionJson() != null && !xform.projectionJson().isBlank()) {
            find = find.projection(BsonDocument.parse(xform.projectionJson()));
        }
        return find.iterator();
    }

    private void streamInto(MongoCursor<RawBsonDocument> cur, PerfSpec perf) throws InterruptedException {
        List<RawBsonDocument> buf = new ArrayList<>(perf.batchDocs());
        long bufBytes = 0;

        while (cur.hasNext() && !ctx.stopping()) {
            awaitUnpaused();
            ctx.rateLimiter().acquire(1);

            RawBsonDocument d = cur.next();
            buf.add(d);
            bufBytes += d.getByteBuffer().remaining();

            if (buf.size() >= perf.batchDocs() || bufBytes >= perf.batchBytes()) {
                enqueue(buf, bufBytes);
                buf = new ArrayList<>(perf.batchDocs());
                bufBytes = 0;
            }
        }
        if (!buf.isEmpty()) enqueue(buf, bufBytes);
    }

    /**
     * Parse a JSON array of aggregation stages. The input is the raw string that
     * came off the wire (either YAML-decoded list-form or a literal JSON array).
     */
    private static List<Bson> parsePipeline(String json) {
        String trimmed = json.trim();
        if (!trimmed.startsWith("[")) {
            throw new IllegalArgumentException(
                    "sourceAggregationJson must be a JSON array of stages, got: " + trimmed);
        }
        // Wrap in a single-field document so we can leverage the driver's BSON parser
        // without pulling in a separate JSON-array-of-documents reader.
        BsonDocument wrapper = BsonDocument.parse("{\"stages\":" + trimmed + "}");
        List<Bson> out = new ArrayList<>();
        for (org.bson.BsonValue v : wrapper.getArray("stages")) {
            if (!v.isDocument()) {
                throw new IllegalArgumentException("aggregation stage is not a document: " + v);
            }
            out.add(v.asDocument());
        }
        return out;
    }

    private Bson buildFilter() {
        List<Bson> clauses = new ArrayList<>(3);
        if (xform != null && xform.filterJson() != null && !xform.filterJson().isBlank()) {
            clauses.add(BsonDocument.parse(xform.filterJson()));
        }
        Bson range = partition.toFilter();
        if (range != null) clauses.add(range);
        if (resumeAfterId != null) clauses.add(Filters.gt("_id", resumeAfterId));
        if (clauses.isEmpty()) return new BsonDocument();
        if (clauses.size() == 1) return clauses.get(0);
        return Filters.and(clauses);
    }

    private void enqueue(List<RawBsonDocument> docs, long bytes) throws InterruptedException {
        BsonValue lastId = docs.get(docs.size() - 1).get("_id");
        Batch b = new Batch(List.copyOf(docs), bytes, lastId);
        while (!queue.offer(b, 250, TimeUnit.MILLISECONDS)) {
            if (ctx.stopping()) return;
        }
    }

    private void awaitUnpaused() throws InterruptedException {
        while (ctx.paused() && !ctx.stopping()) {
            Thread.sleep(100);
        }
    }

    static ReadPreference readPreference(String name) {
        if (name == null) return ReadPreference.primary();
        return switch (name) {
            case "primaryPreferred"   -> ReadPreference.primaryPreferred();
            case "secondary"          -> ReadPreference.secondary();
            case "secondaryPreferred" -> ReadPreference.secondaryPreferred();
            case "nearest"            -> ReadPreference.nearest();
            default                   -> ReadPreference.primary();
        };
    }
}
