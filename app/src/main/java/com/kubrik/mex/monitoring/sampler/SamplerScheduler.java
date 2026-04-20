package com.kubrik.mex.monitoring.sampler;

import com.kubrik.mex.monitoring.MonitoringProfile;
import com.kubrik.mex.monitoring.model.MetricSample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Owns one virtual thread per (connection, sampler kind). Each thread runs a
 * tight loop that: acquires the per-connection {@link AdaptiveLimiter},
 * invokes {@link Sampler#sample}, feeds results through a consumer, and sleeps
 * for the sampler's effective interval (doubled while {@link BackoffTracker}
 * reports back-off).
 *
 * <p>Lifecycle: {@link #register} adds a sampler and starts its loop. {@link #stop}
 * interrupts every loop for that connection and drops state. The scheduler itself
 * is not a singleton — one instance per process is fine (virtual threads are
 * cheap) but multiple instances for tests are also supported.
 */
public final class SamplerScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SamplerScheduler.class);

    private final ExecutorService exec =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("mex-mon-", 0).factory());

    private final Map<String, AdaptiveLimiter> limiters = new ConcurrentHashMap<>();
    private final Map<Key, Future<?>> loops = new ConcurrentHashMap<>();

    private final Consumer<List<MetricSample>> sink;
    private final BiConsumer<Sampler, Throwable> errorSink;

    public SamplerScheduler(Consumer<List<MetricSample>> sink,
                            BiConsumer<Sampler, Throwable> errorSink) {
        this.sink = sink;
        this.errorSink = errorSink;
    }

    /** Start a sampler loop. Idempotent: a second register for the same key is ignored. */
    public void register(MonitoringProfile profile, Sampler sampler) {
        Key key = new Key(sampler.connectionId(), sampler.kind());
        loops.computeIfAbsent(key, k -> {
            AdaptiveLimiter limiter = limiters.computeIfAbsent(k.connectionId, c -> new AdaptiveLimiter());
            return exec.submit(() -> runLoop(profile, sampler, limiter));
        });
    }

    /** Stop every sampler for this connection. */
    public void stop(String connectionId) {
        loops.entrySet().removeIf(e -> {
            if (e.getKey().connectionId.equals(connectionId)) {
                e.getValue().cancel(true);
                return true;
            }
            return false;
        });
        limiters.remove(connectionId);
    }

    /** Stop one sampler kind for this connection, leaving the others running. Idempotent. */
    public void stopOne(String connectionId, SamplerKind kind) {
        Key k = new Key(connectionId, kind);
        Future<?> f = loops.remove(k);
        if (f != null) f.cancel(true);
    }

    /** True if a sampler of {@code kind} is currently running for {@code connectionId}. */
    public boolean isRunning(String connectionId, SamplerKind kind) {
        Future<?> f = loops.get(new Key(connectionId, kind));
        return f != null && !f.isDone();
    }

    /** Stop every sampler (global shutdown). */
    @Override
    public void close() {
        loops.values().forEach(f -> f.cancel(true));
        loops.clear();
        limiters.clear();
        exec.shutdownNow();
    }

    int activeLoops() { return loops.size(); }

    private void runLoop(MonitoringProfile profile, Sampler sampler, AdaptiveLimiter limiter) {
        BackoffTracker backoff = new BackoffTracker();
        Duration baseInterval = profile.instancePollInterval(); // per-kind override below
        while (!Thread.currentThread().isInterrupted()) {
            long startMs = System.currentTimeMillis();
            long startNanos = System.nanoTime();
            boolean acquired = limiter.tryAcquire();
            try {
                if (acquired) {
                    List<MetricSample> samples;
                    try {
                        samples = sampler.sample(Instant.ofEpochMilli(startMs));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    } catch (Throwable t) {
                        long elapsed = (System.nanoTime() - startNanos) / 1_000_000L;
                        backoff.record(elapsed);
                        safeReportError(sampler, t);
                        continue;
                    }
                    long elapsed = (System.nanoTime() - startNanos) / 1_000_000L;
                    backoff.record(elapsed);
                    if (samples != null && !samples.isEmpty()) {
                        try {
                            sink.accept(samples);
                        } catch (Throwable t) {
                            safeReportError(sampler, t);
                        }
                    }
                }
            } finally {
                if (acquired) limiter.release();
            }
            Duration eff = backoff.effectiveInterval(intervalFor(sampler.kind(), profile, baseInterval));
            long sleepMs = Math.max(0L, eff.toMillis() - (System.currentTimeMillis() - startMs));
            try {
                if (sleepMs > 0) Thread.sleep(sleepMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Map a sampler kind to its effective cadence.
     *
     * <p>The profile exposes three user-tunable knobs — {@code instancePollInterval} for
     * the hot-path {@code serverStatus} sampler and the tailing profiler, {@code
     * storagePollInterval} for {@code dbStats} / {@code collStats}, and {@code
     * indexUsagePollInterval} for {@code $indexStats}. Samplers outside those three
     * families keep their kind-default cadence per technical-spec §4.2.
     */
    private static Duration intervalFor(SamplerKind kind, MonitoringProfile p, Duration fallback) {
        return switch (kind) {
            case SERVER_STATUS, PROFILER -> p.instancePollInterval();
            case DB_STATS, COLL_STATS    -> p.storagePollInterval();
            case INDEX_STATS             -> p.indexUsagePollInterval();
            case REPL_STATUS, TOP, CURRENT_OP, OPLOG, SHARDING, METADATA ->
                    kind.defaultInterval();
        };
    }

    private void safeReportError(Sampler sampler, Throwable t) {
        try {
            errorSink.accept(sampler, t);
        } catch (Throwable ignored) {
            log.warn("errorSink threw for {} on {}", sampler.kind(), sampler.connectionId(), ignored);
        }
    }

    private record Key(String connectionId, SamplerKind kind) {}
}
