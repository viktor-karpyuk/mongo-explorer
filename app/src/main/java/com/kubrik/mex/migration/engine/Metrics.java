package com.kubrik.mex.migration.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.LongAdder;

/** Lightweight, thread-safe counters plus a 10-second rolling throughput window.
 *  Uses {@link LongAdder} for the hot write-side counters (OBS-6 P2.5) — lower contention than
 *  {@link java.util.concurrent.atomic.AtomicLong} when many writer threads increment per batch. */
public final class Metrics {

    private final LongAdder docs = new LongAdder();
    private final LongAdder bytes = new LongAdder();
    private final LongAdder errors = new LongAdder();
    /** SCOPE-12 (§4.3): users the UsersCopier stage failed to apply. Non-zero → the job degrades
     *  to {@code COMPLETED_WITH_WARNINGS}. Does not short-circuit the job. */
    private final LongAdder usersFailed = new LongAdder();
    /** OBS-5: documents that cleared the reader + transformer (counted before the write).
     *  Equals {@link #docs} in RUN mode (writer increments both indirectly in dry-run via
     *  identity transform) but in DRY_RUN mode {@code docs} stays 0 — the UI binds to this
     *  counter for the "Docs" column so dry-run progress isn't permanently zero. */
    private final LongAdder docsProcessed = new LongAdder();
    private final Instant startedAt = Instant.now();

    private volatile long windowStartNanos = System.nanoTime();
    private volatile long windowDocs = 0L;
    private volatile long windowBytes = 0L;
    private volatile double rollingDocsPerSec = 0.0;
    private volatile double rollingMbPerSec = 0.0;

    public void addDocs(long n) {
        docs.add(n);
        synchronized (this) { windowDocs += n; maybeTickRolling(); }
    }

    public void addBytes(long n) {
        bytes.add(n);
        synchronized (this) { windowBytes += n; maybeTickRolling(); }
    }

    public void addDocsProcessed(long n) { docsProcessed.add(n); }

    public void addError() { errors.increment(); }

    public void addUserFailed() { usersFailed.increment(); }

    public long docs()          { return docs.sum(); }
    public long bytes()         { return bytes.sum(); }
    public long errors()        { return errors.sum(); }
    public long docsProcessed() { return docsProcessed.sum(); }
    public long usersFailed()   { return usersFailed.sum(); }

    public Duration elapsed() { return Duration.between(startedAt, Instant.now()); }

    public double docsPerSecRolling() { synchronized (this) { return rollingDocsPerSec; } }
    public double mbPerSecRolling()   { synchronized (this) { return rollingMbPerSec; } }

    private void maybeTickRolling() {
        long now = System.nanoTime();
        long windowNanos = now - windowStartNanos;
        if (windowNanos < 10_000_000_000L) return;   // keep window ≥ 10 s for stable readings
        double secs = windowNanos / 1e9;
        rollingDocsPerSec = windowDocs / secs;
        rollingMbPerSec   = (windowBytes / 1_048_576.0) / secs;
        windowStartNanos = now;
        windowDocs = 0L;
        windowBytes = 0L;
    }
}
