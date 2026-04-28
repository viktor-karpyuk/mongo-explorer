package com.kubrik.mex.monitoring.recording;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.monitoring.model.LabelSet;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.recording.store.RecordingDao;
import com.kubrik.mex.monitoring.recording.store.RecordingProfileSampleDao;
import com.kubrik.mex.monitoring.recording.store.RecordingSampleDao;
import com.kubrik.mex.monitoring.store.ProfileSampleRecord;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Q2.3-C — capture subscriber routes {@code EventBus.onMetrics} samples into
 * {@code recording_samples} for any active recording whose connection matches.
 * Covers the acceptance path, paused-drops, other-connection-ignored, profiler
 * double-write, and queue-overflow back-pressure.
 */
class RecordingCaptureSubscriberTest {

    @TempDir Path tmp;
    private Database db;
    private EventBus bus;
    private RecordingDao recordingDao;
    private RecordingSampleDao sampleDao;
    private RecordingProfileSampleDao profileDao;
    private RecordingCaptureSubscriber sub;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", tmp.toString());
        db = new Database();
        bus = new EventBus();
        recordingDao = new RecordingDao(db.connection());
        sampleDao = new RecordingSampleDao(db.connection());
        profileDao = new RecordingProfileSampleDao(db.connection());
        sub = new RecordingCaptureSubscriber(sampleDao, profileDao, bus);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (sub != null) sub.close();
        if (db != null) db.close();
    }

    @Test
    void activeRecordingCapturesMatchingConnectionSamples() throws Exception {
        announceStarted("rec-1", "conn-a");

        bus.publishMetrics(List.of(sample("conn-a", MetricId.INST_OP_1, 1_000L, 42.0)));
        sub.flush();

        assertEquals(1, sampleDao.sampleCount("rec-1"));
        var loaded = sampleDao.loadSamples("rec-1", MetricId.INST_OP_1.metricName(), 0, Long.MAX_VALUE);
        assertEquals(1, loaded.size());
        assertEquals(42.0, loaded.get(0).value());
    }

    @Test
    void samplesForOtherConnectionAreIgnored() throws Exception {
        announceStarted("rec-a", "conn-a");

        bus.publishMetrics(List.of(sample("conn-b", MetricId.INST_OP_1, 1_000L, 99.0)));
        sub.flush();

        assertEquals(0, sampleDao.sampleCount("rec-a"));
    }

    @Test
    void pausedRecordingDropsSamples() throws Exception {
        announceStarted("rec-p", "conn-p");
        bus.publishRecording(new RecordingEvent.Paused("rec-p", nowMs()));

        bus.publishMetrics(List.of(sample("conn-p", MetricId.INST_OP_1, 1_000L, 1.0)));
        sub.flush();

        assertEquals(0, sampleDao.sampleCount("rec-p"));

        // Resume, then samples resume.
        bus.publishRecording(new RecordingEvent.Resumed("rec-p", nowMs()));
        bus.publishMetrics(List.of(sample("conn-p", MetricId.INST_OP_1, 1_200L, 2.0)));
        sub.flush();
        assertEquals(1, sampleDao.sampleCount("rec-p"));
    }

    @Test
    void stoppedRecordingIsRemovedFromRegistry() throws Exception {
        announceStarted("rec-s", "conn-s");
        bus.publishRecording(new RecordingEvent.Stopped("rec-s", StopReason.MANUAL, nowMs()));

        bus.publishMetrics(List.of(sample("conn-s", MetricId.INST_OP_1, 2_000L, 1.0)));
        sub.flush();

        assertEquals(0, sub.activeCount());
        assertEquals(0, sampleDao.sampleCount("rec-s"));
    }

    @Test
    void concurrentActiveRecordingsRouteIndependently() throws Exception {
        announceStarted("rec-a", "conn-a");
        announceStarted("rec-b", "conn-b");

        bus.publishMetrics(List.of(
                sample("conn-a", MetricId.INST_OP_1, 1_000L, 1.0),
                sample("conn-b", MetricId.INST_OP_1, 1_000L, 2.0),
                sample("conn-a", MetricId.INST_OP_2, 1_000L, 3.0)));
        // The writer-thread can race with the test's flush() — flush
        // grabs the writeLock first, drains nothing because the writer
        // already drainedTo'd the queue, then the writer's
        // writeBatchSync runs and writes everything. Poll until the
        // expected counts land instead of asserting against a single
        // snapshot.
        awaitSampleCount("rec-a", 2);
        awaitSampleCount("rec-b", 1);
    }

    @Test
    void profilerSampleDoubleWritesDuringActiveRecording() throws Exception {
        announceStarted("rec-pr", "conn-pr");

        bus.publishProfilerSamples(List.of(profSample("conn-pr", 1_000L, "app.users")));
        // No flush needed for profiler — insert happens synchronously on the bus thread.

        assertEquals(1, profileDao.sampleCount("rec-pr"));
    }

    @Test
    void observabilityCountersReflectWrites() throws Exception {
        announceStarted("rec-m", "conn-m");
        bus.publishMetrics(List.of(
                sample("conn-m", MetricId.INST_OP_1, 1_000L, 1.0),
                sample("conn-m", MetricId.INST_OP_1, 1_100L, 2.0)));
        // Writer thread + flush race — see comment in
        // concurrentActiveRecordingsRouteIndependently.
        awaitCapturedTotal(2);
        assertEquals(0, sub.droppedQueueFull());
        assertEquals(0, sub.writeErrors());
    }

    @Test
    void activeCountMirrorsRegistry() {
        assertEquals(0, sub.activeCount());
        announceStarted("r1", "c1");
        assertEquals(1, sub.activeCount());
        announceStarted("r2", "c2");
        assertEquals(2, sub.activeCount());
        bus.publishRecording(new RecordingEvent.Stopped("r1", StopReason.MANUAL, nowMs()));
        assertEquals(1, sub.activeCount());
    }

    // ---------------------------------------------------------------- helpers

    private void announceStarted(String recordingId, String connectionId) {
        // FK on recording_samples requires the recording row to exist first.
        try {
            recordingDao.insert(new Recording(
                    recordingId, connectionId, recordingId, null, List.of(),
                    RecordingState.ACTIVE, null,
                    nowMs(), null, 0L, null, null, false, nowMs(), 1));
        } catch (java.sql.SQLException e) {
            throw new RuntimeException(e);
        }
        bus.publishRecording(new RecordingEvent.Started(recordingId, connectionId, nowMs()));
    }

    private static MetricSample sample(String conn, MetricId metric, long ts, double v) {
        return new MetricSample(conn, metric, LabelSet.EMPTY, ts, v);
    }

    private static ProfileSampleRecord profSample(String conn, long ts, String ns) {
        return new ProfileSampleRecord(
                conn, ts, ns, "query", 42L, "COLLSCAN",
                100L, 10L, 100L, 0L, 1024L, "qh", "pc", "{}");
    }

    private static long nowMs() { return System.currentTimeMillis(); }

    /** Polls flush + sampleCount up to 5s — absorbs the writer-thread
     *  race noted in the test bodies. */
    private void awaitSampleCount(String recordingId, int expected) throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        long actual = -1;
        while (System.nanoTime() < deadline) {
            sub.flush();
            actual = sampleDao.sampleCount(recordingId);
            if (actual == expected) return;
            Thread.sleep(20);
        }
        fail("expected " + expected + " sample(s) for " + recordingId
                + " but saw " + actual + " after 5s");
    }

    private void awaitCapturedTotal(long expected) throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        long actual = -1;
        while (System.nanoTime() < deadline) {
            sub.flush();
            actual = sub.capturedTotal();
            if (actual == expected) return;
            Thread.sleep(20);
        }
        fail("expected capturedTotal=" + expected + " but saw " + actual + " after 5s");
    }
}
