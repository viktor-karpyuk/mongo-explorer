package com.kubrik.mex.monitoring.recording;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.monitoring.recording.RecordingException.RecordingServiceException;
import com.kubrik.mex.monitoring.recording.store.RecordingDao;
import com.kubrik.mex.monitoring.recording.store.RecordingSampleDao;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Q2.3-B — lifecycle, start rejections, auto-stop, shutdown/crash convergence.
 *
 * <p>Uses an injected {@link MutableClock} so elapsed-time checks are deterministic
 * without waiting real seconds.
 */
class RecordingServiceTest {

    @TempDir Path tmp;
    private Database db;
    private EventBus bus;
    private MutableClock clock;
    private AtomicBoolean connected;
    private AtomicLong capBytes;
    private RecordingService svc;
    private List<RecordingEvent> events;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", tmp.toString());
        db = new Database();
        bus = new EventBus();
        clock = new MutableClock(Instant.ofEpochMilli(1_000));
        connected = new AtomicBoolean(true);
        capBytes = new AtomicLong(Long.MAX_VALUE);
        svc = new RecordingService(db, bus, clock, id -> connected.get(), capBytes::get);
        events = new CopyOnWriteArrayList<>();
        bus.onRecording(events::add);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (svc != null) svc.close();
        if (db != null) db.close();
    }

    // ---------------------------------------------------------------- Lifecycle

    @Test
    void startRoundtripsAndEmitsStarted() throws Exception {
        Recording r = svc.start("conn-1", args("release-42"));
        assertEquals(RecordingState.ACTIVE, r.state());
        assertEquals("conn-1", r.connectionId());
        assertEquals("release-42", r.name());
        assertEquals(1_000L, r.startedAtMs());

        assertEquals(1, events.size());
        assertTrue(events.get(0) instanceof RecordingEvent.Started);

        // Persisted.
        assertTrue(svc.get(r.id()).isPresent());
        assertEquals(1, svc.list().size());
    }

    @Test
    void pauseThenResumeAccumulatesPausedMillis() throws Exception {
        Recording r = svc.start("c", args("p"));

        clock.advance(500);   // 500 ms active
        svc.pause(r.id());

        clock.advance(2_000); // 2 s paused
        svc.resume(r.id());

        Recording reloaded = svc.get(r.id()).orElseThrow();
        assertEquals(RecordingState.ACTIVE, reloaded.state());
        assertEquals(2_000L, reloaded.pausedMillis());

        // Effective duration should be wall elapsed (2500) - paused (2000) = 500
        clock.advance(100);
        assertEquals(600L, reloaded.effectiveDurationMs(clock.millis()));

        assertTrue(events.stream().anyMatch(e -> e instanceof RecordingEvent.Paused));
        assertTrue(events.stream().anyMatch(e -> e instanceof RecordingEvent.Resumed));
    }

    @Test
    void stopSetsEndedAtAndPublishesStopped() throws Exception {
        Recording r = svc.start("c", args("s"));
        clock.advance(750);
        Recording stopped = svc.stop(r.id(), StopReason.MANUAL);

        assertEquals(RecordingState.STOPPED, stopped.state());
        assertEquals(StopReason.MANUAL, stopped.stopReason());
        assertEquals(1_750L, stopped.endedAtMs());

        var last = events.get(events.size() - 1);
        assertTrue(last instanceof RecordingEvent.Stopped);
        assertEquals(StopReason.MANUAL, ((RecordingEvent.Stopped) last).reason());
    }

    @Test
    void stopWhilePausedAccumulatesFinalInterval() throws Exception {
        Recording r = svc.start("c", args("sp"));
        clock.advance(500);
        svc.pause(r.id());
        clock.advance(3_000);   // final pause interval never got resume()'d
        svc.stop(r.id(), StopReason.MANUAL);

        Recording after = svc.get(r.id()).orElseThrow();
        assertEquals(3_000L, after.pausedMillis());
        // ended_at is wall-clock when stop() fires — the paused interval is accounted for
        // in pausedMillis, not by rewinding ended_at.
        assertEquals(4_500L, after.endedAtMs());
        assertEquals(500L, after.effectiveDurationMs(after.endedAtMs()));
    }

    @Test
    void pauseAndResumeAreIdempotent() throws Exception {
        Recording r = svc.start("c", args("idem"));
        svc.pause(r.id());
        svc.pause(r.id());    // no-op, not an error
        svc.resume(r.id());
        svc.resume(r.id());   // no-op
        assertEquals(RecordingState.ACTIVE, svc.get(r.id()).orElseThrow().state());
    }

    @Test
    void stopOfStoppedIsNoOp() throws Exception {
        Recording r = svc.start("c", args("n"));
        svc.stop(r.id(), StopReason.MANUAL);
        Recording again = svc.stop(r.id(), StopReason.AUTO_DURATION);  // shouldn't change reason
        assertEquals(StopReason.MANUAL, again.stopReason());
    }

    @Test
    void renameAndEditNoteAndTagsAfterStop() throws Exception {
        Recording r = svc.start("c", args("orig"));
        svc.stop(r.id(), StopReason.MANUAL);
        svc.rename(r.id(), "renamed");
        svc.editNote(r.id(), "postmortem");
        svc.editTags(r.id(), List.of("release-42", "canary"));

        Recording after = svc.get(r.id()).orElseThrow();
        assertEquals("renamed", after.name());
        assertEquals("postmortem", after.note());
        assertEquals(List.of("release-42", "canary"), after.tags());
        assertEquals(RecordingState.STOPPED, after.state());
    }

    @Test
    void deleteRemovesRow() throws Exception {
        Recording r = svc.start("c", args("gone"));
        svc.delete(r.id());
        assertTrue(svc.get(r.id()).isEmpty());
    }

    // ---------------------------------------------------------------- Rejection

    @Test
    void startOnDisconnectedThrowsConnectionNotConnected() {
        connected.set(false);
        var ex = assertThrows(RecordingServiceException.class, () -> svc.start("c", args("x")));
        assertTrue(ex.reason() instanceof RecordingException.ConnectionNotConnected);
    }

    @Test
    void secondStartOnSameConnectionThrowsAlreadyActive() throws Exception {
        svc.start("c", args("first"));
        var ex = assertThrows(RecordingServiceException.class, () -> svc.start("c", args("second")));
        assertTrue(ex.reason() instanceof RecordingException.RecordingAlreadyActive);
    }

    @Test
    void concurrentStartOnDifferentConnectionsSucceeds() throws Exception {
        Recording a = svc.start("conn-a", args("a"));
        Recording b = svc.start("conn-b", args("b"));
        assertEquals(2, svc.list().size());
        assertNotEquals(a.id(), b.id());
    }

    @Test
    void startWhenStorageCapExceededThrows() {
        capBytes.set(0);   // estimateBytes(0) >= 0 → reject
        var ex = assertThrows(RecordingServiceException.class, () -> svc.start("c", args("x")));
        assertTrue(ex.reason() instanceof RecordingException.StorageCapExceeded);
    }

    @Test
    void invalidStartArgsRejectedBeforeHittingDb() {
        assertThrows(IllegalArgumentException.class,
                () -> svc.start("c", new StartArgs("", null, null, null, null, false)));
        assertThrows(IllegalArgumentException.class,
                () -> svc.start("c", new StartArgs("ok", null, null, -1L, null, false)));
    }

    // ---------------------------------------------------------------- Auto-stop

    @Test
    void autoStopFiresWhenMaxDurationElapsed() throws Exception {
        Recording r = svc.start("c", new StartArgs(
                "capped", null, null, 1_000L, null, false));

        clock.advance(500);
        svc.onTick();
        assertEquals(RecordingState.ACTIVE, svc.get(r.id()).orElseThrow().state());

        clock.advance(600);   // total effective = 1100 ≥ 1000
        svc.onTick();
        Recording after = svc.get(r.id()).orElseThrow();
        assertEquals(RecordingState.STOPPED, after.state());
        assertEquals(StopReason.AUTO_DURATION, after.stopReason());
    }

    @Test
    void autoStopPausedRecordingRespectsPausedMillis() throws Exception {
        Recording r = svc.start("c", new StartArgs(
                "capped", null, null, 1_000L, null, false));

        clock.advance(400);
        svc.pause(r.id());

        clock.advance(10_000);  // 10 s paused must NOT count
        svc.onTick();
        assertEquals(RecordingState.PAUSED, svc.get(r.id()).orElseThrow().state());

        svc.resume(r.id());
        clock.advance(700);     // now 400 + 700 = 1100 effective
        svc.onTick();
        assertEquals(RecordingState.STOPPED, svc.get(r.id()).orElseThrow().state());
    }

    @Test
    void autoStopFiresWhenMaxSizeExceeded() throws Exception {
        Recording r = svc.start("c", new StartArgs(
                "sized", null, null, null, /*maxSizeBytes*/ 100L, false));

        // Write samples until estimateBytes crosses 100 (12-char labels + 28/row = 40 per row → 3 rows = 120)
        var sampleDao = new RecordingSampleDao(db.connection());
        sampleDao.insertBatch(List.of(
                new RecordingWriteTask(r.id(), "c", "m", "{\"db\":\"app\"}", 1_100L, 1.0),
                new RecordingWriteTask(r.id(), "c", "m", "{\"db\":\"app\"}", 1_200L, 2.0),
                new RecordingWriteTask(r.id(), "c", "m", "{\"db\":\"app\"}", 1_300L, 3.0)));
        svc.onTick();

        Recording after = svc.get(r.id()).orElseThrow();
        assertEquals(RecordingState.STOPPED, after.state());
        assertEquals(StopReason.AUTO_SIZE, after.stopReason());
    }

    @Test
    void onTickWithoutAnyRecordingsIsNoOp() {
        assertDoesNotThrow(() -> svc.onTick());
    }

    // ---------------------------------------------------------------- Crash / shutdown

    @Test
    void initSweepsActiveAndPausedFromPriorCrash() throws Exception {
        // Simulate a crash: insert ACTIVE + PAUSED rows directly, then start a new service.
        RecordingDao daoRaw = new RecordingDao(db.connection());
        long now = clock.millis();
        daoRaw.insert(new Recording(
                "pre-active", "c", "a", null, List.of(),
                RecordingState.ACTIVE, null, now, null, 0L, null, null, false, now, 1));
        daoRaw.insert(new Recording(
                "pre-paused", "c", "p", null, List.of(),
                RecordingState.PAUSED, null, now, null, 0L, null, null, false, now, 1));
        daoRaw.insert(new Recording(
                "pre-stopped", "c", "s", null, List.of(),
                RecordingState.STOPPED, StopReason.MANUAL,
                now, now + 50, 0L, null, null, false, now, 1));

        svc.init();

        assertEquals(StopReason.INTERRUPTED, svc.get("pre-active").orElseThrow().stopReason());
        assertEquals(StopReason.INTERRUPTED, svc.get("pre-paused").orElseThrow().stopReason());
        assertEquals(StopReason.MANUAL,      svc.get("pre-stopped").orElseThrow().stopReason());

        // Idempotent — a second init is harmless.
        assertDoesNotThrow(() -> svc.init());
    }

    @Test
    void closeStopsActiveAsInterrupted() throws Exception {
        Recording a = svc.start("c-a", args("a"));
        Recording b = svc.start("c-b", args("b"));
        svc.pause(b.id());

        svc.close();

        // Re-open the same DB and verify the rows converged.
        try (Database db2 = new Database()) {
            RecordingDao dao2 = new RecordingDao(db2.connection());
            Recording reloadedA = dao2.findById(a.id()).orElseThrow();
            Recording reloadedB = dao2.findById(b.id()).orElseThrow();
            assertEquals(RecordingState.STOPPED, reloadedA.state());
            assertEquals(StopReason.INTERRUPTED, reloadedA.stopReason());
            assertEquals(RecordingState.STOPPED, reloadedB.state());
            assertEquals(StopReason.INTERRUPTED, reloadedB.stopReason());
        }
        svc = null;   // prevent double-close in tearDown
    }

    // ---------------------------------------------------------------- helpers

    private static StartArgs args(String name) {
        return new StartArgs(name, null, null, null, null, false);
    }

    /** Deterministic in-memory clock for unit tests. */
    static final class MutableClock extends Clock {
        private Instant now;
        MutableClock(Instant start) { this.now = start; }
        void advance(long millis) { now = now.plusMillis(millis); }
        @Override public ZoneId getZone() { return ZoneId.of("UTC"); }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
        @Override public long millis() { return now.toEpochMilli(); }
    }
}
