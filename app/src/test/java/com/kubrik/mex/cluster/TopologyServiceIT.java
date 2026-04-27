package com.kubrik.mex.cluster;

import com.kubrik.mex.cluster.model.ClusterKind;
import com.kubrik.mex.cluster.model.MemberState;
import com.kubrik.mex.cluster.model.TopologySnapshot;
import com.kubrik.mex.cluster.service.ClusterTopologyService;
import com.kubrik.mex.cluster.store.TopologySnapshotDao;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Path;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.4 TOPO-1..9 acceptance — runs the topology sampler against a live
 * {@code mongo:latest} replica set. The Testcontainers {@code MongoDBContainer}
 * starts a single-node replset by default; the full 3-node matrix (step-down,
 * lag, elections) lives in the hardening soak suite (Q2.4-J). This IT covers
 * the REPLSET code path end-to-end: kind detection, primary identification,
 * snapshot persistence, sha256 de-dup, and EventBus fan-out.
 */
@Testcontainers(disabledWithoutDocker = true)
class TopologyServiceIT {

    @Container
    static MongoDBContainer MONGO = new MongoDBContainer("mongo:latest");

    @TempDir Path dataDir;

    private Database db;
    private EventBus bus;
    private MongoService mongo;
    private ClusterTopologyService topology;
    private TopologySnapshotDao dao;
    private final String connectionId = "topo-it";

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        bus = new EventBus();
        mongo = new MongoService(MONGO.getConnectionString());
        dao = new TopologySnapshotDao(db);
        topology = new ClusterTopologyService(
                id -> connectionId.equals(id) ? mongo : null,
                dao, bus, Clock.systemUTC());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (topology != null) topology.close();
        if (mongo != null) mongo.close();
        if (db != null) db.close();
    }

    @Test
    void detects_replset_kind_and_primary() {
        TopologySnapshot snap = topology.refreshNow(connectionId);
        // refreshNow requires an active state — start() first.
        assertNull(snap, "refreshNow before start should return null");

        topology.start(connectionId);
        snap = topology.refreshNow(connectionId);
        assertNotNull(snap, "snapshot after start");
        assertEquals(ClusterKind.REPLSET, snap.clusterKind());
        assertFalse(snap.members().isEmpty(), "replset must report >= 1 member");
        assertTrue(
                snap.members().stream().anyMatch(m -> m.state() == MemberState.PRIMARY),
                "exactly one PRIMARY expected on a single-node replset");
        assertFalse(snap.version().isBlank(), "version present");
    }

    @Test
    void sha256_is_stable_across_identical_samples() {
        topology.start(connectionId);
        TopologySnapshot first = topology.refreshNow(connectionId);
        assertNotNull(first);
        // Structural JSON (capturedAt-independent) must be byte-equal for two
        // consecutive samples with no cluster change.
        TopologySnapshot second = topology.refreshNow(connectionId);
        assertNotNull(second);
        assertEquals(first.structuralCanonicalJson(), second.structuralCanonicalJson(),
                "structural sha should not drift between identical snapshots");
    }

    @Test
    void dao_dedups_identical_snapshots_by_sha256() {
        topology.start(connectionId);
        TopologySnapshot first = topology.refreshNow(connectionId);
        assertNotNull(first);
        // Insert the SAME captured snapshot twice — the second call must be a no-op
        // because sha256 matches the row the sampler just persisted. (A fresh
        // refreshNow can produce a different optime because replsets keep
        // heartbeat-writing to the oplog, which is why this test uses the
        // already-captured value rather than a second live sample.)
        long dup = dao.insertIfChanged(connectionId, first);
        assertEquals(-1L, dup, "identical snapshot must be deduped by sha256");
    }

    @Test
    void event_bus_delivers_to_live_subscribers() throws Exception {
        AtomicInteger received = new AtomicInteger();
        AtomicReference<TopologySnapshot> last = new AtomicReference<>();
        var sub = bus.onTopology((id, snap) -> {
            if (connectionId.equals(id)) {
                received.incrementAndGet();
                last.set(snap);
            }
        });
        try {
            topology.start(connectionId);
            topology.refreshNow(connectionId);
            assertTrue(received.get() >= 1, "at least one tick delivered");
            assertNotNull(last.get());
            assertEquals(ClusterKind.REPLSET, last.get().clusterKind());
        } finally {
            sub.close();
        }
    }
}
