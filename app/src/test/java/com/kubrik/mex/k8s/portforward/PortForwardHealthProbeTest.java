package com.kubrik.mex.k8s.portforward;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.k8s.cluster.KubeClusterDao;
import com.kubrik.mex.k8s.events.PortForwardEvent;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.model.PortForwardSession;
import com.kubrik.mex.k8s.model.PortForwardTarget;
import com.kubrik.mex.store.Database;
import io.kubernetes.client.openapi.ApiClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.1 Q2.8-N — Exercises the probe loop the port-forward service
 * spawns when {@code probeIntervalSeconds > 0}. Uses deterministic
 * stub {@link PortForwardService.HealthProbe}s so the test doesn't
 * need a live Kubernetes API server.
 */
class PortForwardHealthProbeTest {

    @TempDir Path dataDir;
    private Database db;
    private PortForwardAuditDao dao;
    private long clusterId;
    private EventBus bus;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        dao = new PortForwardAuditDao(db);
        clusterId = new KubeClusterDao(db).insert(
                "t", "/k", "ctx", Optional.empty(), Optional.empty());
        bus = new EventBus();
    }

    @AfterEach
    void tearDown() throws Exception { if (db != null) db.close(); }

    @Test
    void probe_closes_session_with_auth_expired_on_401() throws Exception {
        runAndAssertClose(
                client -> PortForwardService.HealthProbe.Outcome.AUTH_EXPIRED,
                PortForwardService.REASON_AUTH_EXPIRED);
    }

    @Test
    void probe_closes_session_with_health_fail_on_network_error() throws Exception {
        runAndAssertClose(
                client -> PortForwardService.HealthProbe.Outcome.UNHEALTHY,
                PortForwardService.REASON_HEALTH_FAIL);
    }

    @Test
    void healthy_probe_keeps_session_open() throws Exception {
        PortForwardService svc = build(
                client -> PortForwardService.HealthProbe.Outcome.HEALTHY,
                1L);
        PortForwardSession s = svc.open(ref(clusterId), "conn",
                PortForwardTarget.forPod("mongo", "p", 27017));
        // Give the probe loop at least two ticks to run without
        // closing the session.
        Thread.sleep(2_500);
        assertEquals(1, svc.openSessionIds().size(),
                "a HEALTHY probe must not close the tunnel");
        svc.close();
    }

    private void runAndAssertClose(PortForwardService.HealthProbe probe,
                                    String expectedReason) throws Exception {
        AtomicReference<PortForwardEvent.Closed> closedEvt = new AtomicReference<>();
        bus.onPortForward(e -> {
            if (e instanceof PortForwardEvent.Closed c) closedEvt.set(c);
        });

        PortForwardService svc = build(probe, 1L);
        PortForwardSession s = svc.open(ref(clusterId), "conn",
                PortForwardTarget.forPod("mongo", "p", 27017));

        // Poll up to 6 s for the probe tick to fire and close the
        // session (sleep + probe + close dispatch).
        long deadline = System.currentTimeMillis() + 6_000;
        while (System.currentTimeMillis() < deadline) {
            if (closedEvt.get() != null) break;
            Thread.sleep(100);
        }
        assertNotNull(closedEvt.get(),
                "probe loop should close the session and emit a Closed event");
        assertEquals(expectedReason, closedEvt.get().reason());
        assertEquals(0, svc.openSessionIds().size());

        try (ResultSet rs = db.connection().createStatement()
                .executeQuery("SELECT reason_closed FROM portforward_audit "
                        + "WHERE id = " + s.auditRowId())) {
            assertTrue(rs.next());
            assertEquals(expectedReason, rs.getString("reason_closed"));
        }
        svc.close();
    }

    private PortForwardService build(PortForwardService.HealthProbe probe,
                                       long intervalSeconds) {
        return new PortForwardService(
                new PortForwardServiceTest.TestClientFactory(),
                dao, bus,
                new PortForwardServiceTest.CapturingOpener(),
                probe,
                intervalSeconds);
    }

    private static K8sClusterRef ref(long id) {
        return new K8sClusterRef(id, "t", "/k", "ctx",
                Optional.empty(), Optional.empty(), 0L, Optional.empty());
    }
}
