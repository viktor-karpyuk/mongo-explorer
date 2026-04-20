package com.kubrik.mex.monitoring;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.model.ConnectionState;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exercises the {@link MonitoringWiring} cycle without a real {@code ConnectionManager}:
 * the wiring listens for {@link ConnectionState} events on the shared {@link EventBus}.
 * {@code ConnectionManager.service(id)} returns {@code null} in this test (we don't have
 * a live Mongo handy), so sampler registration is skipped — but the profile persistence
 * and the enabled-flag flip on reconnect are exercised.
 */
class MonitoringWiringTest {

    @TempDir Path tmp;
    private Database db;
    private EventBus bus;
    private MonitoringService svc;
    private com.kubrik.mex.core.ConnectionManager connManager;
    private MonitoringWiring wiring;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", tmp.toString());
        db = new Database();
        bus = new EventBus();
        svc = new MonitoringService(db, bus);
        com.kubrik.mex.store.ConnectionStore store = new com.kubrik.mex.store.ConnectionStore(db);
        connManager = new com.kubrik.mex.core.ConnectionManager(
                store, bus, new com.kubrik.mex.core.Crypto());
        wiring = new MonitoringWiring(svc, connManager, bus);
    }

    @AfterEach
    void tearDown() throws Exception {
        wiring.close();
        svc.close();
        db.close();
    }

    @Test
    void connectedEventPersistsProfileWithEnabledTrue() throws Exception {
        bus.publishState(new ConnectionState("c-xyz", ConnectionState.Status.CONNECTED, "7.0.0", null));
        // Profile is upserted even though we can't register samplers (service(null)).
        var profile = svc.profile("c-xyz").orElseThrow();
        assertTrue(profile.enabled());
    }

    @Test
    void disconnectFlipsEnabledFalseThenReconnectFlipsItBack() throws Exception {
        bus.publishState(new ConnectionState("c-xyz", ConnectionState.Status.CONNECTED, "7.0.0", null));
        bus.publishState(new ConnectionState("c-xyz", ConnectionState.Status.DISCONNECTED, null, null));
        assertFalse(readEnabledFlag("c-xyz"), "disconnect should flip enabled=false on disk");
        bus.publishState(new ConnectionState("c-xyz", ConnectionState.Status.CONNECTED, "7.0.0", null));
        assertTrue(readEnabledFlag("c-xyz"), "reconnect should flip enabled=true");
    }

    @Test
    void splitDropCountersStartAtZero() {
        assertEquals(0, svc.metricStore().droppedQueueFull());
        assertEquals(0, svc.metricStore().droppedSqlError());
        assertEquals(0, svc.metricStore().droppedSamples(),
                "aggregate dropped counter is the sum of the two buckets");
    }

    @Test
    void alertTopicDeliversFiredEvents() throws Exception {
        java.util.concurrent.atomic.AtomicInteger fired = new java.util.concurrent.atomic.AtomicInteger();
        bus.onAlertFired(e -> fired.incrementAndGet());
        // Install a zero-sustain GT rule on WT_3 and push one sample above crit.
        var rule = new com.kubrik.mex.monitoring.alerting.AlertRule(
                "rule-test", null, MetricId.WT_3,
                java.util.Map.of(),
                com.kubrik.mex.monitoring.alerting.Comparator.GT,
                0.90, 0.97, java.time.Duration.ZERO, true, null);
        svc.upsertRule(rule);
        bus.publishMetrics(java.util.List.of(new MetricSample(
                "c-xyz", MetricId.WT_3,
                com.kubrik.mex.monitoring.model.LabelSet.EMPTY, 1_000, 0.98)));
        assertEquals(1, fired.get(),
                "event bus alert.fired topic must receive the transition");
    }

    private boolean readEnabledFlag(String connectionId) throws Exception {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT enabled FROM monitoring_profiles WHERE connection_id = ?")) {
            ps.setString(1, connectionId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "row for " + connectionId + " must exist");
                return rs.getInt("enabled") != 0;
            }
        }
    }
}
