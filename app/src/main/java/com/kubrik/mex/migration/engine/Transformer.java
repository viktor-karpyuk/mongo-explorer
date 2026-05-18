package com.kubrik.mex.migration.engine;

import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/** Pulls batches from the reader queue, applies the compiled transform, forwards to the writer
 *  queue. Per-document cast failures are logged and counted but do not kill the pipeline. */
public final class Transformer {

    private static final Logger log = LoggerFactory.getLogger(Transformer.class);

    private final JobContext ctx;
    private final BsonTransform transform;
    private final BlockingQueue<Batch> in;
    private final BlockingQueue<Batch> out;
    private final String sourceNs;

    public Transformer(JobContext ctx,
                       BsonTransform transform,
                       BlockingQueue<Batch> in,
                       BlockingQueue<Batch> out,
                       String sourceNs) {
        this.ctx = ctx;
        this.transform = transform;
        this.in = in;
        this.out = out;
        this.sourceNs = sourceNs;
    }

    public void run() throws InterruptedException {
        while (true) {
            Batch batch = in.take();
            if (batch.isPoison()) {
                out.put(Batch.POISON);
                return;
            }
            if (ctx.stopping()) {
                out.put(Batch.POISON);
                return;
            }
            if (transform.isIdentity()) {
                ctx.metrics().addDocsProcessed(batch.size());
                offer(batch);
                continue;
            }
            List<RawBsonDocument> transformed = new ArrayList<>(batch.docs().size());
            long bytes = 0;
            for (RawBsonDocument doc : batch.docs()) {
                try {
                    RawBsonDocument t = transform.apply(doc);
                    transformed.add(t);
                    bytes += t.getByteBuffer().remaining();
                } catch (PerDocumentException e) {
                    ctx.metrics().addError();
                    log.warn("{} transform failed for doc {}: {}", sourceNs, e.docId(), e.getMessage());
                    // drop the document; continues batching the rest
                }
            }
            if (!transformed.isEmpty()) {
                ctx.metrics().addDocsProcessed(transformed.size());
                Batch xb = new Batch(List.copyOf(transformed), bytes, batch.lastId());
                offer(xb);
            }
        }
    }

    private void offer(Batch b) throws InterruptedException {
        while (!out.offer(b, 250, TimeUnit.MILLISECONDS)) {
            if (ctx.stopping()) return;
        }
    }
}
