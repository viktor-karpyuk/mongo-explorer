package com.kubrik.mex.monitoring.recording;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.monitoring.recording.RecordingException.ConnectionNotConnected;
import com.kubrik.mex.monitoring.recording.RecordingException.RecordingAlreadyActive;
import com.kubrik.mex.monitoring.recording.RecordingException.RecordingNotFound;
import com.kubrik.mex.monitoring.recording.RecordingException.StorageCapExceeded;
import com.kubrik.mex.monitoring.recording.store.RecordingDao;
import com.kubrik.mex.monitoring.recording.store.RecordingSampleDao;
import com.kubrik.mex.store.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

/**
 * Lifecycle facade for the Monitoring Recording subsystem — see technical-spec §5.
 *
 * <p>Wires together {@link RecordingDao}, {@link RecordingSampleDao}, the
 * injected {@link Clock}, and a single-thread {@link ScheduledExecutorService}
 * that enforces auto-stop every 5 seconds.
 *
 * <p>Concurrency: every state-changing method acquires a single
 * {@link ReentrantLock}, so start/pause/resume/stop and the scheduled
 * auto-stop tick never race on the same recording.
 *
 * <p>Clean-shutdown / crash convergence: {@link #close()} stops every
 * {@code ACTIVE}/{@code PAUSED} recording with {@link StopReason#INTERRUPTED}
 * synchronously. If the JVM exits before {@code close()} runs, the next launch
 * applies the same {@code INTERRUPTED} terminal via {@link #init()} calling
 * {@link RecordingDao#sweepInterrupted()} — both paths reach the same on-disk
 * state.
 */
public final class RecordingService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RecordingService.class);

    /** Default recording storage cap (2 GiB) when no other value is wired up. */
    public static final long DEFAULT_STORAGE_CAP_BYTES = 2L * 1024 * 1024 * 1024;

    /** Scheduler tick period for auto-stop enforcement. */
    static final long TICK_PERIOD_MS = 5_000;

    private final RecordingDao dao;
    private final RecordingSampleDao sampleDao;
    private final EventBus bus;
    private final Clock clock;
    private final Predicate<String> isConnected;
    private final LongSupplier storageCapBytes;
    private final ScheduledExecutorService scheduler;

    private final ReentrantLock lock = new ReentrantLock();
    /** Records the wall time when a recording entered {@code PAUSED} so resume() can
     *  accumulate the paused interval into {@code Recording.pausedMillis}. */
    private final Map<String, Long> pauseStartedAt = new HashMap<>();

    private volatile boolean started;

    public RecordingService(Database database, EventBus bus) {
        this(database, bus, Clock.systemUTC(), id -> true, () -> DEFAULT_STORAGE_CAP_BYTES);
    }

    public RecordingService(Database database, EventBus bus, Clock clock,
                            Predicate<String> isConnected, LongSupplier storageCapBytes) {
        this.dao = new RecordingDao(database.connection());
        this.sampleDao = new RecordingSampleDao(database.connection());
        this.bus = bus;
        this.clock = clock;
        this.isConnected = isConnected;
        this.storageCapBytes = storageCapBytes;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "recording-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Run the one-shot crash-recovery sweep and start the auto-stop tick.
     * Idempotent — safe to call multiple times.
     */
    public void init() throws SQLException {
        if (started) return;
        started = true;
        int swept = dao.sweepInterrupted();
        if (swept > 0) log.info("recording crash-recovery sweep marked {} row(s) INTERRUPTED", swept);
        scheduler.scheduleAtFixedRate(this::tickSafely,
                TICK_PERIOD_MS, TICK_PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    // ---------------------------------------------------------------- Lifecycle

    public Recording start(String connectionId, StartArgs args) throws SQLException {
        lock.lock();
        try {
            if (!isConnected.test(connectionId)) {
                throw new ConnectionNotConnected(connectionId).asRuntime();
            }
            List<Recording> active = dao.findActiveForConnection(connectionId);
            if (!active.isEmpty()) {
                throw new RecordingAlreadyActive(connectionId, active.get(0).name()).asRuntime();
            }
            long cap = storageCapBytes.getAsLong();
            long used = sampleDao.estimateBytes();
            if (used >= cap) {
                throw new StorageCapExceeded(used, cap).asRuntime();
            }
            long now = clock.millis();
            Recording r = new Recording(
                    UUID.randomUUID().toString(),
                    connectionId,
                    args.name(),
                    args.note(),
                    args.tags(),
                    RecordingState.ACTIVE,
                    null,
                    now,
                    null,
                    0L,
                    args.maxDurationMs(),
                    args.maxSizeBytes(),
                    args.captureProfilerSamples(),
                    now,
                    1);
            dao.insert(r);
            bus.publishRecording(new RecordingEvent.Started(r.id(), connectionId, now));
            return r;
        } finally {
            lock.unlock();
        }
    }

    public void pause(String recordingId) throws SQLException {
        lock.lock();
        try {
            Recording r = requireFound(recordingId);
            if (r.state() != RecordingState.ACTIVE) {
                return;   // idempotent: already paused or stopped
            }
            long now = clock.millis();
            Recording paused = r.withTransition(RecordingState.PAUSED, null, null, r.pausedMillis());
            dao.update(paused);
            pauseStartedAt.put(recordingId, now);
            bus.publishRecording(new RecordingEvent.Paused(recordingId, now));
        } finally {
            lock.unlock();
        }
    }

    public void resume(String recordingId) throws SQLException {
        lock.lock();
        try {
            Recording r = requireFound(recordingId);
            if (r.state() != RecordingState.PAUSED) {
                return;   // idempotent: already active or stopped
            }
            long now = clock.millis();
            Long pauseStart = pauseStartedAt.remove(recordingId);
            long additional = pauseStart != null ? Math.max(0, now - pauseStart) : 0L;
            Recording resumed = r.withTransition(RecordingState.ACTIVE, null, null,
                    r.pausedMillis() + additional);
            dao.update(resumed);
            bus.publishRecording(new RecordingEvent.Resumed(recordingId, now));
        } finally {
            lock.unlock();
        }
    }

    public Recording stop(String recordingId, StopReason reason) throws SQLException {
        lock.lock();
        try {
            Recording r = requireFound(recordingId);
            if (r.state() == RecordingState.STOPPED) return r;
            long now = clock.millis();
            long pausedMillis = r.pausedMillis();
            if (r.state() == RecordingState.PAUSED) {
                Long pauseStart = pauseStartedAt.remove(recordingId);
                if (pauseStart != null) pausedMillis += Math.max(0, now - pauseStart);
            }
            Recording stopped = r.withTransition(RecordingState.STOPPED, reason, now, pausedMillis);
            dao.update(stopped);
            bus.publishRecording(new RecordingEvent.Stopped(recordingId, reason, now));
            return stopped;
        } finally {
            lock.unlock();
        }
    }

    public void delete(String recordingId) throws SQLException {
        lock.lock();
        try {
            requireFound(recordingId);
            dao.delete(recordingId);
            pauseStartedAt.remove(recordingId);
        } finally {
            lock.unlock();
        }
    }

    public void rename(String recordingId, String newName) throws SQLException {
        mutateMetadata(recordingId, r -> r.withName(newName));
    }

    public void editNote(String recordingId, String note) throws SQLException {
        mutateMetadata(recordingId, r -> r.withNote(note));
    }

    public void editTags(String recordingId, List<String> tags) throws SQLException {
        mutateMetadata(recordingId, r -> r.withTags(tags));
    }

    // ---------------------------------------------------------------- Queries

    public Optional<Recording> get(String recordingId) throws SQLException {
        return dao.findById(recordingId);
    }

    public List<Recording> list() throws SQLException {
        return dao.listAll();
    }

    public List<Recording> activeForConnection(String connectionId) throws SQLException {
        return dao.findActiveForConnection(connectionId);
    }

    // ---------------------------------------------------------------- Scheduler tick

    /**
     * Package-private so tests can drive time without waiting 5s of real clock.
     * Enforces {@link StartArgs#maxDurationMs} and {@link StartArgs#maxSizeBytes}.
     */
    void onTick() {
        long now = clock.millis();
        List<Recording> all;
        try {
            all = dao.listAll();
        } catch (SQLException e) {
            log.warn("recording auto-stop tick: list failed", e);
            return;
        }
        for (Recording r : all) {
            if (r.state() != RecordingState.ACTIVE && r.state() != RecordingState.PAUSED) continue;
            // Freeze effective time at pause-start for PAUSED recordings so the in-progress
            // pause interval doesn't silently count toward the max-duration limit.
            long effectiveNow = now;
            if (r.state() == RecordingState.PAUSED) {
                Long pauseStart = pauseStartedAt.get(r.id());
                if (pauseStart != null) effectiveNow = pauseStart;
            }
            try {
                if (r.maxDurationMs() != null && r.effectiveDurationMs(effectiveNow) >= r.maxDurationMs()) {
                    stop(r.id(), StopReason.AUTO_DURATION);
                    continue;
                }
                if (r.maxSizeBytes() != null) {
                    long used = sampleDao.estimateBytes(r.id());
                    if (used >= r.maxSizeBytes()) {
                        stop(r.id(), StopReason.AUTO_SIZE);
                    }
                }
            } catch (SQLException e) {
                log.warn("recording auto-stop failed for {}: {}", r.id(), e.toString());
            }
        }
    }

    private void tickSafely() {
        try { onTick(); } catch (Throwable t) { log.warn("recording scheduler tick failed", t); }
    }

    // ---------------------------------------------------------------- Shutdown

    /**
     * Stop every {@code ACTIVE}/{@code PAUSED} recording with
     * {@link StopReason#INTERRUPTED} and shut the scheduler down. If the JVM
     * skips this (SIGKILL, OOM), the next {@link #init()} converges to the
     * same state via {@link RecordingDao#sweepInterrupted()}.
     */
    @Override
    public void close() {
        scheduler.shutdown();
        try { scheduler.awaitTermination(5, TimeUnit.SECONDS); }
        catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
        lock.lock();
        try {
            List<Recording> all = dao.listAll();
            for (Recording r : all) {
                if (r.state() == RecordingState.ACTIVE || r.state() == RecordingState.PAUSED) {
                    try { stop(r.id(), StopReason.INTERRUPTED); }
                    catch (SQLException e) { log.warn("recording close: stop failed for {}", r.id(), e); }
                }
            }
        } catch (SQLException e) {
            log.warn("recording close: list failed", e);
        } finally {
            lock.unlock();
        }
    }

    // ---------------------------------------------------------------- Helpers

    private Recording requireFound(String recordingId) throws SQLException {
        return dao.findById(recordingId)
                .orElseThrow(() -> new RecordingNotFound(recordingId).asRuntime());
    }

    private void mutateMetadata(String recordingId, java.util.function.UnaryOperator<Recording> f)
            throws SQLException {
        lock.lock();
        try {
            // Metadata (name, note, tags) is editable in any state per REC-META-1/2/3.
            Recording r = requireFound(recordingId);
            dao.update(f.apply(r));
        } finally {
            lock.unlock();
        }
    }
}
