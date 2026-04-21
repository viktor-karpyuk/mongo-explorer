package com.kubrik.mex.monitoring.recording;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.recording.store.RecordingProfileSampleDao;
import com.kubrik.mex.monitoring.recording.store.RecordingSampleDao;
import com.kubrik.mex.monitoring.store.ProfileSampleRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Double-writes {@link MetricSample} bus events into {@code recording_samples}
 * for every active recording whose connection matches — see technical-spec §4.
 *
 * <p>Design mirrors {@link com.kubrik.mex.monitoring.store.MetricStore}:
 * bounded queue, single writer thread, 200 ms batched flush, drop counter on
 * overflow. Live monitoring never blocks on this subscriber — overflow drops
 * silently and bumps {@link #droppedQueueFull()}.
 */
public final class RecordingCaptureSubscriber implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RecordingCaptureSubscriber.class);

    public static final int QUEUE_CAPACITY = 16_384;
    public static final long FLUSH_INTERVAL_MS = 200;
    public static final int MAX_BATCH = 1_024;

    private final RecordingSampleDao sampleDao;
    private final RecordingProfileSampleDao profileDao;
    private final EventBus bus;

    /** Active registry keyed by connection id. Visible-to-tests. */
    private final ConcurrentMap<String, ActiveRecording> byConnection = new ConcurrentHashMap<>();
    /** Parallel index so pause/resume/stop events (which carry only recordingId) resolve in O(1). */
    private final ConcurrentMap<String, ActiveRecording> byRecordingId = new ConcurrentHashMap<>();

    private final BlockingQueue<RecordingWriteTask> writerQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

    private final ExecutorService writer = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "recording-writer");
        t.setDaemon(true);
        return t;
    });

    private final AtomicLong capturedTotal    = new AtomicLong();
    private final AtomicLong droppedQueueFull = new AtomicLong();
    private final AtomicLong writeErrors      = new AtomicLong();
    private final AtomicLong lastFlushMs      = new AtomicLong();
    private volatile boolean running = true;
    /** Shared with every other component that writes to the same SQLite connection
     *  (auto-stop tick, future sidecar writers). See {@code Database.writeLock()}
     *  and docs/v2/v2.5/fixes-v2.3-recording-hardening.md B1/B3. */
    private final Object writeLock;

    private final EventBus.Subscription recordingSub;
    private final EventBus.Subscription metricsSub;
    private final EventBus.Subscription profilerSub;

    /** Back-compat ctor for tests + standalone dev — creates a private write-lock. */
    public RecordingCaptureSubscriber(RecordingSampleDao sampleDao,
                                      RecordingProfileSampleDao profileDao,
                                      EventBus bus) {
        this(sampleDao, profileDao, bus, new Object());
    }

    public RecordingCaptureSubscriber(RecordingSampleDao sampleDao,
                                      RecordingProfileSampleDao profileDao,
                                      EventBus bus,
                                      Object writeLock) {
        this.sampleDao = sampleDao;
        this.profileDao = profileDao;
        this.bus = bus;
        this.writeLock = writeLock;
        this.recordingSub = bus.onRecording(this::onRecordingEvent);
        this.metricsSub   = bus.onMetrics(this::onSamples);
        this.profilerSub  = bus.onProfilerSamples(this::onProfilerSamples);
        writer.submit(this::writeLoop);
    }

    // --------------------------------------------------------------- Lifecycle wiring

    private void onRecordingEvent(RecordingEvent e) {
        if (e instanceof RecordingEvent.Started s) {
            ActiveRecording ar = new ActiveRecording(s.recordingId(), s.connectionId());
            // The service rejects concurrent active recordings per connection; if a
            // Started event arrives for a connection we already track, keep the existing
            // entry and surface the collision at WARN so it's diagnosable (B8).
            ActiveRecording prior = byConnection.putIfAbsent(s.connectionId(), ar);
            if (prior != null) {
                log.warn("recording Started collision on {}: keeping {} ignoring {}",
                        s.connectionId(), prior.recordingId, s.recordingId());
            } else {
                byRecordingId.put(s.recordingId(), ar);
            }
        } else if (e instanceof RecordingEvent.Paused p) {
            ActiveRecording ar = byRecordingId.get(p.recordingId());
            if (ar != null) ar.paused = true;
        } else if (e instanceof RecordingEvent.Resumed r) {
            ActiveRecording ar = byRecordingId.get(r.recordingId());
            if (ar != null) ar.paused = false;
        } else if (e instanceof RecordingEvent.Stopped s) {
            ActiveRecording ar = byRecordingId.remove(s.recordingId());
            if (ar != null) byConnection.remove(ar.connectionId, ar);
        }
    }

    // --------------------------------------------------------------- Sample routing

    void onSamples(List<MetricSample> samples) {
        if (samples == null || samples.isEmpty() || byConnection.isEmpty()) return;
        for (MetricSample s : samples) {
            ActiveRecording ar = byConnection.get(s.connectionId());
            if (ar == null || ar.paused) continue;
            RecordingWriteTask t = new RecordingWriteTask(
                    ar.recordingId, s.connectionId(),
                    s.metric().metricName(), s.labels().toJson(),
                    s.tsMs(), s.value());
            if (!writerQueue.offer(t)) {
                droppedQueueFull.incrementAndGet();
            }
        }
    }

    void onProfilerSamples(List<ProfileSampleRecord> samples) {
        if (samples == null || samples.isEmpty() || byConnection.isEmpty()) return;
        // Group first (no DB access; safe to do outside writeLock).
        Map<String, List<ProfileSampleRecord>> byRecording = new LinkedHashMap<>();
        for (ProfileSampleRecord p : samples) {
            ActiveRecording ar = byConnection.get(p.connectionId());
            if (ar == null || ar.paused) continue;
            byRecording.computeIfAbsent(ar.recordingId, k -> new ArrayList<>()).add(p);
        }
        if (byRecording.isEmpty()) return;
        // One insertBatch per recording, all under the shared writeLock so we
        // don't interleave transactions with the metric writer thread on the
        // shared SQLite connection (B1).
        synchronized (writeLock) {
            for (Map.Entry<String, List<ProfileSampleRecord>> e : byRecording.entrySet()) {
                try {
                    profileDao.insertBatch(e.getKey(), e.getValue());
                } catch (SQLException ex) {
                    writeErrors.addAndGet(e.getValue().size());
                    log.warn("recording profiler double-write failed for {}: {}", e.getKey(), ex.toString());
                    bus.publishRecording(new RecordingEvent.SampleWriteError(
                            e.getKey(), ex.getMessage(), System.currentTimeMillis()));
                }
            }
        }
    }

    // --------------------------------------------------------------- Writer

    private void writeLoop() {
        List<RecordingWriteTask> batch = new ArrayList<>(MAX_BATCH);
        while (running) {
            try {
                RecordingWriteTask first = writerQueue.poll(FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
                if (first == null) continue;
                batch.add(first);
                writerQueue.drainTo(batch, MAX_BATCH - 1);
                long t0 = System.nanoTime();
                writeBatchSync(batch);
                lastFlushMs.set((System.nanoTime() - t0) / 1_000_000);
                batch.clear();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                log.warn("recording writer loop error", t);
            }
        }
    }

    private void writeBatchSync(List<RecordingWriteTask> batch) {
        if (batch.isEmpty()) return;
        synchronized (writeLock) {
            try {
                int wrote = sampleDao.insertBatch(batch);
                capturedTotal.addAndGet(wrote);
            } catch (SQLException e) {
                writeErrors.addAndGet(batch.size());
                log.warn("recording batch write failed, dropped {} samples: {}", batch.size(), e.toString());
                RecordingWriteTask first = batch.get(0);
                bus.publishRecording(new RecordingEvent.SampleWriteError(
                        first.recordingId(), e.getMessage(), System.currentTimeMillis()));
            }
        }
    }

    /** Force-drain any queued tasks synchronously. For tests + clean shutdown. */
    public void flush() {
        // Wait out any in-flight writer-thread batch before draining — eliminates the
        // races that produced intermittent "0 samples written" failures in CI.
        synchronized (writeLock) {
            List<RecordingWriteTask> batch = new ArrayList<>(writerQueue.size());
            writerQueue.drainTo(batch, MAX_BATCH);
            if (!batch.isEmpty()) {
                try {
                    int wrote = sampleDao.insertBatch(batch);
                    capturedTotal.addAndGet(wrote);
                } catch (SQLException e) {
                    writeErrors.addAndGet(batch.size());
                    log.warn("recording flush write failed, dropped {} samples: {}", batch.size(), e.toString());
                }
            }
        }
    }

    // --------------------------------------------------------------- Observability

    public long capturedTotal()     { return capturedTotal.get(); }
    public long droppedQueueFull()  { return droppedQueueFull.get(); }
    public long writeErrors()       { return writeErrors.get(); }
    public long queueDepth()        { return writerQueue.size(); }
    public long lastFlushMillis()   { return lastFlushMs.get(); }

    /** Active-recordings count; handy for tests/UI instrumentation. */
    public int activeCount() { return byConnection.size(); }

    // --------------------------------------------------------------- Teardown

    @Override
    public void close() {
        running = false;
        try { recordingSub.close(); } catch (Exception ignored) {}
        try { metricsSub.close();   } catch (Exception ignored) {}
        try { profilerSub.close();  } catch (Exception ignored) {}
        writer.shutdown();
        try { writer.awaitTermination(5, TimeUnit.SECONDS); }
        catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
        flush();   // drain whatever landed after the loop exited
        byConnection.clear();
        byRecordingId.clear();
    }

    // --------------------------------------------------------------- Registry entry

    /** Registry entry. Not a record because {@code paused} is mutated under volatile semantics. */
    static final class ActiveRecording {
        final String recordingId;
        final String connectionId;
        volatile boolean paused;
        ActiveRecording(String recordingId, String connectionId) {
            this.recordingId = recordingId;
            this.connectionId = connectionId;
        }
    }
}
