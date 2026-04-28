package com.kubrik.mex.migration.sink;

import com.kubrik.mex.migration.engine.Batch;
import com.kubrik.mex.migration.engine.JobContext;
import com.kubrik.mex.migration.engine.Namespaces;
import com.kubrik.mex.migration.spec.ExecutionMode;
import org.bson.BsonValue;

import java.util.concurrent.BlockingQueue;
import java.util.function.BiConsumer;

/** Drains the transformer's output queue into a {@link MigrationSink} — the single write
 *  stage of every migration after the EXT-1 SPI freeze. Handles dry-run, metrics (docs +
 *  bytes), checkpointing, and stop-flag propagation on behalf of every sink kind so
 *  sinks themselves stay format-focused. Retry lives inside the sink (see {@code MongoSink}
 *  for the retry-on-transient-error contract on the MongoDB write path). */
public final class SinkWriter {

    private final JobContext ctx;
    private final MigrationSink sink;
    private final Namespaces.Ns targetNs;
    private final BlockingQueue<Batch> in;
    private final BiConsumer<BsonValue, Long> checkpoint;

    public SinkWriter(JobContext ctx,
                      MigrationSink sink,
                      Namespaces.Ns targetNs,
                      BlockingQueue<Batch> in,
                      BiConsumer<BsonValue, Long> checkpoint) {
        this.ctx = ctx;
        this.sink = sink;
        this.targetNs = targetNs;
        this.in = in;
        this.checkpoint = checkpoint;
    }

    public void run() throws Exception {
        boolean dryRun = ctx.spec().options().executionMode() == ExecutionMode.DRY_RUN;
        sink.open(targetNs);
        try {
            while (true) {
                Batch batch = in.take();
                if (batch.isPoison()) return;
                if (ctx.stopping()) return;

                long batchSize = batch.size();
                long batchBytes = batch.bytes();

                if (!dryRun) {
                    sink.writeBatch(batch);
                }
                // Metrics flow in both modes so the UI reflects read+transform throughput
                // exactly like the Mongo-target path (OBS-5).
                ctx.metrics().addDocs(batchSize);
                ctx.metrics().addBytes(batchBytes);
                checkpoint.accept(batch.lastId(), batchSize);
            }
        } finally {
            sink.close();
        }
    }
}
