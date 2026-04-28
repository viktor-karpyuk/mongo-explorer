package com.kubrik.mex.k8s.cluster;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.events.ClusterEvent;
import com.kubrik.mex.k8s.model.ClusterProbeResult;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class KubeClusterServiceTest {

    @TempDir Path dataDir;

    private Database db;
    private KubeClusterDao dao;
    private EventBus bus;
    private FakeProbe probe;
    private KubeClusterService svc;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        dao = new KubeClusterDao(db);
        bus = new EventBus();
        probe = new FakeProbe();
        svc = new KubeClusterService(dao, new KubeClientFactory(), probe, bus);
    }

    @AfterEach
    void tearDown() throws Exception { if (db != null) db.close(); }

    @Test
    void add_publishes_cluster_added_event() throws SQLException {
        AtomicReference<ClusterEvent> seen = new AtomicReference<>();
        bus.onKubeCluster(seen::set);
        K8sClusterRef added = svc.add("dev", "/k1", "ctx", Optional.empty(), Optional.empty());
        assertNotNull(seen.get());
        assertInstanceOf(ClusterEvent.Added.class, seen.get());
        assertEquals(added.id(), seen.get().clusterId());
    }

    @Test
    void probe_publishes_result_and_touches_row_when_reachable() throws SQLException {
        K8sClusterRef r = svc.add("dev", "/k1", "ctx", Optional.empty(), Optional.empty());
        List<ClusterEvent> seen = new ArrayList<>();
        bus.onKubeCluster(seen::add);
        probe.next = new ClusterProbeResult(
                ClusterProbeResult.Status.REACHABLE,
                Optional.of("v1.31.0"),
                Optional.of(3),
                Optional.empty(),
                System.currentTimeMillis());

        ClusterProbeResult out = svc.probe(r);
        assertTrue(out.ok());
        assertTrue(seen.stream().anyMatch(e -> e instanceof ClusterEvent.Probed));
        assertTrue(dao.findById(r.id()).orElseThrow().lastUsedAt().isPresent(),
                "reachable probe must bump last_used_at");
    }

    @Test
    void probe_auth_failure_does_not_touch_last_used() throws SQLException {
        K8sClusterRef r = svc.add("dev", "/k1", "ctx", Optional.empty(), Optional.empty());
        probe.next = new ClusterProbeResult(
                ClusterProbeResult.Status.AUTH_FAILED,
                Optional.empty(),
                Optional.empty(),
                Optional.of("HTTP 401"),
                System.currentTimeMillis());
        svc.probe(r);
        assertTrue(dao.findById(r.id()).orElseThrow().lastUsedAt().isEmpty());
    }

    @Test
    void remove_publishes_removed_event() throws SQLException {
        K8sClusterRef r = svc.add("dev", "/k1", "ctx", Optional.empty(), Optional.empty());
        List<ClusterEvent> seen = new ArrayList<>();
        bus.onKubeCluster(seen::add);
        svc.remove(r.id());
        assertTrue(seen.stream().anyMatch(e -> e instanceof ClusterEvent.Removed));
        assertTrue(dao.findById(r.id()).isEmpty());
    }

    /** Test stub. Subclassing is cleaner than mocking — the service only calls {@link ClusterProbeService#probe}. */
    static final class FakeProbe extends ClusterProbeService {
        ClusterProbeResult next = new ClusterProbeResult(
                ClusterProbeResult.Status.UNREACHABLE,
                Optional.empty(),
                Optional.empty(),
                Optional.of("stub"),
                0L);
        FakeProbe() { super(new KubeClientFactory(), 1_000L); }
        @Override public ClusterProbeResult probe(K8sClusterRef ref) { return next; }
    }
}
