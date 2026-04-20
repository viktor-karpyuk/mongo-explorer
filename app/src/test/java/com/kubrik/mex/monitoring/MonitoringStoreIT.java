package com.kubrik.mex.monitoring;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.monitoring.model.LabelSet;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.sampler.Sampler;
import com.kubrik.mex.monitoring.sampler.SamplerKind;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end smoke of the P-1 stack with a fake Sampler (no MongoDB required):
 * profile persistence + scheduler + metric store + event bus.
 */
class MonitoringStoreIT {

    @TempDir Path tmp;
    private Database db;
    private EventBus bus;
    private MonitoringService svc;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", tmp.toString());
        db = new Database();
        bus = new EventBus();
        svc = new MonitoringService(db, bus);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (svc != null) svc.close();
        if (db != null) db.close();
    }

    @Test
    void profileRoundtrips() throws Exception {
        MonitoringProfile p = MonitoringProfile.defaults("conn-1");
        svc.enable(p);
        var loaded = svc.profile("conn-1").orElseThrow();
        assertTrue(loaded.enabled());
        assertEquals("secondaryPreferred", loaded.readPreference());
        assertEquals(Duration.ofSeconds(1), loaded.instancePollInterval());
    }

    @Test
    void samplerPipelineWritesToSqliteAndFiresEventBus() throws Exception {
        List<MetricSample> busSamples = new CopyOnWriteArrayList<>();
        bus.onMetrics(busSamples::addAll);

        MonitoringProfile p = new MonitoringProfile(
                "conn-A", true,
                Duration.ofMillis(80), Duration.ofSeconds(60), Duration.ofMinutes(5),
                "secondaryPreferred",
                false, 100, Duration.ofMinutes(60),
                50, List.of(),
                MonitoringProfile.defaults("conn-A").retention(),
                Instant.now(), Instant.now());
        svc.enable(p);

        CountingFakeSampler sampler = new CountingFakeSampler("conn-A");
        svc.registerSampler(sampler);

        // Wait up to 2s for at least 5 ticks (one every ~80ms).
        long deadline = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < deadline && sampler.invocations.get() < 5) {
            Thread.sleep(25);
        }
        assertTrue(sampler.invocations.get() >= 5,
                "sampler should have been invoked at least 5 times; was " + sampler.invocations.get());

        // Give the writer thread a chance to flush.
        Thread.sleep(300);
        svc.metricStore().flush();

        assertTrue(svc.writtenSamples() > 0, "raw-tier writes should have happened");
        List<MetricSample> loaded = svc.queryRaw(
                "conn-A", MetricId.INST_OP_1, 0, Long.MAX_VALUE / 2);
        assertFalse(loaded.isEmpty(), "stored samples should be queryable");

        // Bus should have received the same samples.
        assertFalse(busSamples.isEmpty(), "event bus should have observed samples");
    }

    @Test
    void disableStopsTheSampler() throws Exception {
        MonitoringProfile p = new MonitoringProfile(
                "conn-B", true,
                Duration.ofMillis(60), Duration.ofSeconds(60), Duration.ofMinutes(5),
                "secondaryPreferred",
                false, 100, Duration.ofMinutes(60),
                50, List.of(),
                MonitoringProfile.defaults("conn-B").retention(),
                Instant.now(), Instant.now());
        svc.enable(p);
        CountingFakeSampler sampler = new CountingFakeSampler("conn-B");
        svc.registerSampler(sampler);

        Thread.sleep(300);
        int before = sampler.invocations.get();
        svc.disable("conn-B");
        Thread.sleep(300);
        int after = sampler.invocations.get();
        assertTrue(after - before <= 2,
                "no further invocations should happen after disable (before=" + before + " after=" + after + ")");
    }

    /** Produces one synthetic INST-OP-1 sample per tick. */
    private static final class CountingFakeSampler implements Sampler {
        private final String connectionId;
        final AtomicInteger invocations = new AtomicInteger();

        CountingFakeSampler(String connectionId) { this.connectionId = connectionId; }

        @Override public SamplerKind kind() { return SamplerKind.SERVER_STATUS; }
        @Override public String connectionId() { return connectionId; }

        @Override public List<MetricSample> sample(Instant now) {
            int n = invocations.incrementAndGet();
            List<MetricSample> out = new ArrayList<>(1);
            out.add(new MetricSample(
                    connectionId, MetricId.INST_OP_1, LabelSet.EMPTY,
                    now.toEpochMilli(), n));
            return Collections.unmodifiableList(out);
        }
    }
}
