package com.kubrik.mex.monitoring.recording;

import com.kubrik.mex.monitoring.recording.store.RecordingDao;
import com.kubrik.mex.monitoring.recording.store.RecordingSampleDao;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Q2.3-A smoke — schema migration, DAO CRUD, cascade delete, and the crash-recovery
 * sweep. Covers the foundation every later phase builds on.
 */
class RecordingDaoTest {

    @TempDir Path tmp;
    private Database db;
    private RecordingDao recordingDao;
    private RecordingSampleDao sampleDao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", tmp.toString());
        db = new Database();
        recordingDao = new RecordingDao(db.connection());
        sampleDao = new RecordingSampleDao(db.connection());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void insertAndReadRoundtrips() throws Exception {
        Recording r = new Recording(
                "rec-1", "conn-1", "release-42 post-deploy", "10% canary",
                List.of("release-42", "post-deploy"),
                RecordingState.ACTIVE, null,
                1_700_000_000_000L, null, 0L,
                1_800_000L, null, true,
                1_700_000_000_000L, 1);
        recordingDao.insert(r);

        Optional<Recording> loaded = recordingDao.findById("rec-1");
        assertTrue(loaded.isPresent());
        Recording got = loaded.get();
        assertEquals("rec-1", got.id());
        assertEquals("conn-1", got.connectionId());
        assertEquals(RecordingState.ACTIVE, got.state());
        assertNull(got.endedAtMs());
        assertNull(got.stopReason());
        assertEquals(List.of("release-42", "post-deploy"), got.tags());
        assertTrue(got.captureProfilerSamples());
        assertEquals(1, got.schemaVersion());
    }

    @Test
    void listAllOrdersByStartedAtDesc() throws Exception {
        recordingDao.insert(active("r-1", "a", 1_000));
        recordingDao.insert(active("r-2", "a", 3_000));
        recordingDao.insert(active("r-3", "a", 2_000));

        List<Recording> all = recordingDao.listAll();
        assertEquals(3, all.size());
        assertEquals("r-2", all.get(0).id());  // newest first
        assertEquals("r-3", all.get(1).id());
        assertEquals("r-1", all.get(2).id());
    }

    @Test
    void findActiveForConnectionSkipsStopped() throws Exception {
        recordingDao.insert(active("r-active", "conn-a", 1_000));
        Recording stopped = new Recording(
                "r-stopped", "conn-a", "old", null, List.of(),
                RecordingState.STOPPED, StopReason.MANUAL,
                500L, 900L, 0L, null, null, false, 500L, 1);
        recordingDao.insert(stopped);
        // Different connection.
        recordingDao.insert(active("r-other", "conn-b", 1_000));

        List<Recording> hits = recordingDao.findActiveForConnection("conn-a");
        assertEquals(1, hits.size());
        assertEquals("r-active", hits.get(0).id());
    }

    @Test
    void sampleBatchInsertRoundtrips() throws Exception {
        recordingDao.insert(active("rec-s", "c-s", 1_000));
        List<RecordingWriteTask> batch = List.of(
                new RecordingWriteTask("rec-s", "c-s", "mongo.ops.insert", "{}", 1_100L, 42.0),
                new RecordingWriteTask("rec-s", "c-s", "mongo.ops.insert", "{}", 1_200L, 43.5),
                new RecordingWriteTask("rec-s", "c-s", "mongo.ops.query",  "{}", 1_200L, 99.0));
        int wrote = sampleDao.insertBatch(batch);
        assertEquals(3, wrote);

        List<RecordedSample> inserts = sampleDao.loadSamples("rec-s", "mongo.ops.insert", 0, Long.MAX_VALUE);
        assertEquals(2, inserts.size());
        assertEquals(42.0, inserts.get(0).value());
        assertEquals(43.5, inserts.get(1).value());

        assertEquals(3, sampleDao.sampleCount("rec-s"));
        assertEquals(2, sampleDao.sampleCount("rec-s", "mongo.ops.insert"));

        List<Series> series = sampleDao.listSeries("rec-s");
        assertEquals(2, series.size());
        assertEquals("mongo.ops.insert", series.get(0).metric());
        assertEquals(2L, series.get(0).sampleCount());
    }

    @Test
    void sampleInsertIsIdempotentOnPrimaryKey() throws Exception {
        recordingDao.insert(active("rec-dup", "c-s", 1_000));
        var t = new RecordingWriteTask("rec-dup", "c-s", "m", "{}", 1_100L, 1.0);
        sampleDao.insertBatch(List.of(t, t));   // same key twice
        assertEquals(1, sampleDao.sampleCount("rec-dup"));

        // Re-insert with a new value should overwrite (INSERT OR REPLACE).
        sampleDao.insertBatch(List.of(new RecordingWriteTask("rec-dup", "c-s", "m", "{}", 1_100L, 7.0)));
        var reloaded = sampleDao.loadSamples("rec-dup", "m", 0, Long.MAX_VALUE);
        assertEquals(1, reloaded.size());
        assertEquals(7.0, reloaded.get(0).value());
    }

    @Test
    void deleteCascadesToSamples() throws Exception {
        recordingDao.insert(active("rec-del", "c", 1_000));
        sampleDao.insertBatch(List.of(
                new RecordingWriteTask("rec-del", "c", "m", "{}", 1_100L, 1.0),
                new RecordingWriteTask("rec-del", "c", "m", "{}", 1_200L, 2.0)));
        assertEquals(2, sampleDao.sampleCount("rec-del"));

        recordingDao.delete("rec-del");

        assertTrue(recordingDao.findById("rec-del").isEmpty());
        assertEquals(0, sampleDao.sampleCount("rec-del"));
    }

    @Test
    void sweepInterruptedMarksActiveAndPausedAsStopped() throws Exception {
        recordingDao.insert(active("rec-active", "c", 1_000));
        recordingDao.insert(paused("rec-paused", "c", 500));
        recordingDao.insert(new Recording(
                "rec-already-stopped", "c", "done", null, List.of(),
                RecordingState.STOPPED, StopReason.MANUAL,
                100L, 200L, 0L, null, null, false, 100L, 1));

        // Give the active recording one sample so ended_at back-fills to its ts.
        sampleDao.insertBatch(List.of(
                new RecordingWriteTask("rec-active", "c", "m", "{}", 1_500L, 1.0)));

        int swept = recordingDao.sweepInterrupted();
        assertEquals(2, swept);

        Recording active = recordingDao.findById("rec-active").orElseThrow();
        assertEquals(RecordingState.STOPPED, active.state());
        assertEquals(StopReason.INTERRUPTED, active.stopReason());
        assertEquals(1_500L, active.endedAtMs());  // backfilled from last sample

        Recording paused = recordingDao.findById("rec-paused").orElseThrow();
        assertEquals(RecordingState.STOPPED, paused.state());
        assertEquals(StopReason.INTERRUPTED, paused.stopReason());
        assertEquals(500L, paused.endedAtMs());    // no samples → started_at fallback

        Recording alreadyStopped = recordingDao.findById("rec-already-stopped").orElseThrow();
        assertEquals(StopReason.MANUAL, alreadyStopped.stopReason());  // untouched

        // Running twice is a no-op.
        assertEquals(0, recordingDao.sweepInterrupted());
    }

    @Test
    void estimateBytesReflectsSampleFootprint() throws Exception {
        recordingDao.insert(active("rec-sz", "c", 1_000));
        assertEquals(0, sampleDao.estimateBytes("rec-sz"));

        sampleDao.insertBatch(List.of(
                new RecordingWriteTask("rec-sz", "c", "m", "{\"db\":\"app\"}", 1_100L, 1.0),
                new RecordingWriteTask("rec-sz", "c", "m", "{\"db\":\"app\"}", 1_200L, 2.0)));

        long bytes = sampleDao.estimateBytes("rec-sz");
        // LENGTH({"db":"app"}) = 12; +28 per row. 2 rows = 12*2 + 28*2 = 80.
        assertEquals(12L * 2 + 28L * 2, bytes);
    }

    @Test
    void tagsRoundtripThroughPersistence() throws Exception {
        Recording r = new Recording(
                "rec-tags", "c", "tagged", null,
                List.of("one", "two-dashed", "three"),
                RecordingState.ACTIVE, null,
                100L, null, 0L, null, null, false, 100L, 1);
        recordingDao.insert(r);
        Recording loaded = recordingDao.findById("rec-tags").orElseThrow();
        assertEquals(List.of("one", "two-dashed", "three"), loaded.tags());

        // Zero tags round-trip as an empty list, not null.
        recordingDao.insert(new Recording(
                "rec-none", "c", "no-tags", null, List.of(),
                RecordingState.ACTIVE, null,
                100L, null, 0L, null, null, false, 100L, 1));
        assertEquals(List.of(), recordingDao.findById("rec-none").orElseThrow().tags());
    }

    // ---------------------------------------------------------------------- helpers

    private static Recording active(String id, String connectionId, long startedAt) {
        return new Recording(id, connectionId, id, null, List.of(),
                RecordingState.ACTIVE, null,
                startedAt, null, 0L, null, null, false, startedAt, 1);
    }

    private static Recording paused(String id, String connectionId, long startedAt) {
        return new Recording(id, connectionId, id, null, List.of(),
                RecordingState.PAUSED, null,
                startedAt, null, 0L, null, null, false, startedAt, 1);
    }
}
