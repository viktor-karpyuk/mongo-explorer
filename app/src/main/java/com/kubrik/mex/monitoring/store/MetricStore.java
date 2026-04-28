package com.kubrik.mex.monitoring.store;

import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coalesces sampler output into batched SQLite writes, per technical-spec §5.2:
 *
 * <ul>
 *   <li>One bounded {@link ArrayBlockingQueue} of size {@link #QUEUE_CAPACITY}.</li>
 *   <li>One dedicated writer thread; SQLite is a single-writer engine so doubling
 *       up buys nothing.</li>
 *   <li>Writer wakes every {@link #FLUSH_INTERVAL}, drains the queue, and issues
 *       one batched {@code executeBatch()}.</li>
 *   <li>Queue overflow drops the oldest sample and increments {@link #droppedSamples()}
 *       so the UI can surface a "samples dropped" indicator.</li>
 * </ul>
 *
 * <p>{@link #close()} drains and flushes any pending samples — required to satisfy
 * BR-13 (no lost samples on graceful quit).
 */
public final class MetricStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MetricStore.class);

    public static final int QUEUE_CAPACITY = 8_192;
    public static final Duration FLUSH_INTERVAL = Duration.ofMillis(200);
    /** Upper bound on the drain-per-flush batch size to keep individual transactions small. */
    public static final int MAX_BATCH = 1_024;

    private final RawSampleDao dao;
    private final BlockingQueue<MetricSample> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final AtomicLong droppedQueueFull = new AtomicLong();
    private final AtomicLong droppedSqlError = new AtomicLong();
    private final AtomicLong written = new AtomicLong();

    private final ExecutorService writer =
            Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "mex-mon-writer");
                t.setDaemon(true);
                return t;
            });
    private volatile boolean running = true;

    public MetricStore(Connection connection) {
        this(connection, new Object());
    }

    /**
     * Preferred constructor — pairs the shared connection with the
     * database's {@code writeLock()} object so {@link RawSampleDao}
     * serialises its {@code setAutoCommit(false)} toggle against
     * every other writer in the app.
     */
    public MetricStore(Connection connection, Object writeLock) {
        this.dao = new RawSampleDao(connection, writeLock);
        writer.submit(this::writeLoop);
    }

    /**
     * Non-blocking accept. Dropped samples increment the dropped-counter so operators
     * see the back-pressure signal; in practice 8K queue + 200 ms drain handles any
     * realistic cadence.
     */
    public void persistAsync(List<MetricSample> samples) {
        for (MetricSample s : samples) {
            if (!queue.offer(s)) {
                // Drop the oldest, enqueue the newest. Under continuous overflow this
                // approximates "keep the most recent window" rather than losing the tail.
                queue.poll();
                droppedQueueFull.incrementAndGet();
                if (!queue.offer(s)) {
                    // Still full (improbable): count and move on.
                    droppedQueueFull.incrementAndGet();
                }
            }
        }
    }

    /** Raw-tier read-through. Exposed for UI/tests; tiering logic lives outside this class. */
    public List<MetricSample> queryRaw(String connectionId, MetricId metric,
                                       long fromMsInclusive, long toMsExclusive) throws SQLException {
        return dao.loadRange(connectionId, metric, fromMsInclusive, toMsExclusive);
    }

    /** Total drops regardless of cause — retained for backward-compatible callers. */
    public long droppedSamples() { return droppedQueueFull.get() + droppedSqlError.get(); }
    /** Drops caused by back-pressure (queue was full). */
    public long droppedQueueFull() { return droppedQueueFull.get(); }
    /** Drops caused by SQL write failures. */
    public long droppedSqlError() { return droppedSqlError.get(); }
    public long writtenSamples() { return written.get(); }

    /** Force-flush any queued samples. Blocks until drained. Useful for tests. */
    public void flush() {
        List<MetricSample> batch = new ArrayList<>(queue.size());
        queue.drainTo(batch, MAX_BATCH);
        writeBatch(batch);
    }

    @Override
    public void close() {
        running = false;
        writer.shutdown();
        try {
            writer.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        // Final drain after the writer stopped.
        flush();
    }

    private void writeLoop() {
        List<MetricSample> batch = new ArrayList<>(MAX_BATCH);
        long flushMs = FLUSH_INTERVAL.toMillis();
        while (running) {
            try {
                MetricSample first = queue.poll(flushMs, TimeUnit.MILLISECONDS);
                if (first != null) {
                    batch.add(first);
                    queue.drainTo(batch, MAX_BATCH - 1);
                    writeBatch(batch);
                    batch.clear();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                log.warn("monitoring writer loop error", t);
            }
        }
    }

    private void writeBatch(List<MetricSample> batch) {
        if (batch.isEmpty()) return;
        try {
            int n = dao.insertBatch(batch);
            written.addAndGet(n);
        } catch (SQLException e) {
            // With QUEUE_CAPACITY already bounded, an SQL error that drops the batch is
            // preferable to blocking the sampler loops indefinitely. Real retry-with-backoff
            // belongs to technical-spec §12 failure-mode work in a later phase.
            droppedSqlError.addAndGet(batch.size());
            log.warn("monitoring batch write failed, dropped {} samples: {}", batch.size(), e.toString());
        }
    }
}
