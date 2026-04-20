package com.kubrik.mex.migration.sink;

import com.kubrik.mex.migration.engine.Batch;
import com.kubrik.mex.migration.engine.Namespaces;
import com.kubrik.mex.migration.engine.RetryPolicy;
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

/** {@link MigrationSink} adapter over a MongoDB {@link MongoCollection}.
 *  <p>
 *  Folds what was the engine's standalone {@code Writer} into the sink contract so both
 *  MongoDB and file destinations share a single pipeline (EXT-1). Retry on transient errors
 *  lives here (previously in {@code Writer}); dry-run + metrics + checkpointing stay in
 *  {@link SinkWriter}, which drives every sink uniformly. */
public final class MongoSink implements MigrationSink {

    private static final Logger log = LoggerFactory.getLogger(MongoSink.class);
    private static final BulkWriteOptions UNORDERED = new BulkWriteOptions().ordered(false);

    private final MongoCollection<RawBsonDocument> target;
    private final ConflictMode mode;
    private final RetryPolicy retry;

    public MongoSink(MongoCollection<RawBsonDocument> target,
                     ConflictMode mode,
                     int retryAttempts) {
        this.target = target;
        this.mode = mode;
        this.retry = new RetryPolicy(retryAttempts, mode);
    }

    @Override
    public void open(Namespaces.Ns target) {
        // MongoCollection is already live — caller wired it up in CollectionPipeline, including
        // drop-and-recreate and unique-index bootstrap. Nothing to do here.
    }

    @Override
    public void writeBatch(Batch batch) throws Exception {
        List<WriteModel<RawBsonDocument>> models = toWriteModels(batch);
        if (models.isEmpty()) return;   // preserves pre-refactor behaviour: empty batch = no-op
        retry.execute("bulkWrite", () -> {
            BulkWriteResult r = target.bulkWrite(models, UNORDERED);
            log.debug("bulk ok: inserted={} upserted={} modified={}",
                    r.getInsertedCount(), r.getUpserts().size(), r.getModifiedCount());
            return r;
        });
    }

    @Override
    public void close() {
        // Collection handle is owned by the MongoService; nothing to release.
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
