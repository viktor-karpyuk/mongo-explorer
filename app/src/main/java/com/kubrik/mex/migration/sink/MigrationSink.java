package com.kubrik.mex.migration.sink;

import com.kubrik.mex.migration.engine.Batch;
import com.kubrik.mex.migration.engine.Namespaces;

/** Destination for a migration's output stream (EXT-1).
 *  <p>
 *  A sink owns its output resource for one namespace: it is opened before the first batch,
 *  fed batches in order by a dedicated consumer thread, and closed exactly once when the
 *  upstream delivers the poison pill (or on error).
 *  <p>
 *  <b>Status:</b> {@code @Beta} — this is an internal seed for the v2.0.0 plugin SPI
 *  ({@code EXT-1}). The signature will stay stable within v2.x but is not yet committed as a
 *  public plugin contract — don't package third-party implementations against it. */
public interface MigrationSink extends AutoCloseable {

    /** Called once on the consumer thread before the first batch. Implementations should
     *  acquire file handles, create directories, etc. */
    void open(Namespaces.Ns target) throws Exception;

    /** Called for every non-poison batch delivered from the transformer. Must be idempotent
     *  at the batch boundary — retries may replay a batch after a transient IO failure. */
    void writeBatch(Batch batch) throws Exception;

    /** Called exactly once after the final batch (or on error). Implementations must flush
     *  and release the output resource here. */
    @Override
    void close() throws Exception;
}
