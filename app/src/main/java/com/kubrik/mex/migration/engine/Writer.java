package com.kubrik.mex.migration.engine;

import com.kubrik.mex.migration.spec.ConflictMode;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.BiConsumer;

/** Consumes batches from the transformer queue and bulk-writes them to the target.
 *  Applies retry on transient errors, checkpoints after each successful bulk. */
public final class Writer {

    private static final Logger log = LoggerFactory.getLogger(Writer.class);
    private static final BulkWriteOptions UNORDERED = new BulkWriteOptions().ordered(false);

    private final JobContext ctx;
    private final MongoCollection<RawBsonDocument> target;
    private final ConflictMode mode;
    private final BlockingQueue<Batch> in;
    private final RetryPolicy retry;
    private final BiConsumer<BsonValue, Long> checkpoint; // (lastId, docsWritten)

    public Writer(JobContext ctx,
                  MongoCollection<RawBsonDocument> target,
                  ConflictMode mode,
                  BlockingQueue<Batch> in,
                  BiConsumer<BsonValue, Long> checkpoint) {
        this.ctx = ctx;
        this.target = target;
        this.mode = mode;
        this.in = in;
        this.retry = new RetryPolicy(ctx.spec().options().performance().retryAttempts(), mode);
        this.checkpoint = checkpoint;
    }

    public void run() throws Exception {
        while (true) {
            Batch batch = in.take();
            if (batch.isPoison()) return;
            if (ctx.stopping()) return;

            List<WriteModel<RawBsonDocument>> models = toWriteModels(batch);
            if (models.isEmpty()) continue;

            long batchSize = batch.size();
            long batchBytes = batch.bytes();

            if (ctx.spec().options().executionMode() == com.kubrik.mex.migration.spec.ExecutionMode.DRY_RUN) {
                // Skip the write; still update metrics so the progress view is realistic.
                ctx.metrics().addDocs(batchSize);
                ctx.metrics().addBytes(batchBytes);
                checkpoint.accept(batch.lastId(), batchSize);
                continue;
            }

            retry.execute("bulkWrite", () -> {
                BulkWriteResult r = target.bulkWrite(models, UNORDERED);
                ctx.metrics().addDocs(batchSize);
                ctx.metrics().addBytes(batchBytes);
                log.debug("bulk ok: inserted={} upserted={} modified={}",
                        r.getInsertedCount(), r.getUpserts().size(), r.getModifiedCount());
                return r;
            });

            checkpoint.accept(batch.lastId(), batchSize);
        }
    }

    private List<WriteModel<RawBsonDocument>> toWriteModels(Batch batch) {
        List<WriteModel<RawBsonDocument>> out = new ArrayList<>(batch.docs().size());
        for (RawBsonDocument doc : batch.docs()) {
            switch (mode) {
                case ABORT, APPEND, DROP_AND_RECREATE -> out.add(new InsertOneModel<>(doc));
                case UPSERT_BY_ID -> {
                    BsonValue id = doc.get("_id");
                    out.add(new ReplaceOneModel<>(Filters.eq("_id", id), doc,
                            new ReplaceOptions().upsert(true)));
                }
            }
        }
        return out;
    }
}
