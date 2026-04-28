package com.kubrik.mex.cluster;

import com.kubrik.mex.cluster.audit.OpsAuditRecord;
import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.model.ClusterKind;
import com.kubrik.mex.cluster.model.TopologySnapshot;
import com.kubrik.mex.cluster.safety.RoleSet;
import com.kubrik.mex.cluster.store.OpsAuditDao;
import com.kubrik.mex.cluster.store.RoleCacheDao;
import com.kubrik.mex.cluster.store.TopologySnapshotDao;
import com.kubrik.mex.model.MongoConnection;
import com.kubrik.mex.store.ConnectionStore;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.4 Q2.4-J.2 — deleting a connection purges {@code topology_snapshots},
 * {@code ops_audit}, and {@code role_cache} rows referencing it while
 * leaving rows for neighbouring connections untouched. Runs in a single
 * transaction so a mid-delete failure rolls all of it back.
 */
class CascadeDeleteIT {

    @TempDir Path dataDir;

    private Database db;
    private ConnectionStore store;
    private OpsAuditDao audit;
    private TopologySnapshotDao topology;
    private RoleCacheDao roleCache;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        store = new ConnectionStore(db);
        audit = new OpsAuditDao(db);
        topology = new TopologySnapshotDao(db);
        roleCache = new RoleCacheDao(db);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void deleting_connection_purges_cluster_tables_for_that_id() {
        store.upsert(conn("cx-target", "target"));
        store.upsert(conn("cx-neighbour", "neighbour"));

        // Seed each of the three v2.4 cluster tables for both connections.
        seedAllTables("cx-target");
        seedAllTables("cx-neighbour");

        assertEquals(2, audit.listForConnection("cx-target", 10).size());
        assertEquals(1, topology.listRecent("cx-target", 10).size());
        assertNotNull(roleCache.load("cx-target"));

        store.delete("cx-target");

        // Target's rows are gone.
        assertEquals(0, audit.listForConnection("cx-target", 10).size(),
                "ops_audit rows must be purged");
        assertEquals(0, topology.listRecent("cx-target", 10).size(),
                "topology_snapshots rows must be purged");
        assertNull(roleCache.load("cx-target"),
                "role_cache row must be purged");

        // Neighbour is untouched.
        assertEquals(2, audit.listForConnection("cx-neighbour", 10).size());
        assertEquals(1, topology.listRecent("cx-neighbour", 10).size());
        assertNotNull(roleCache.load("cx-neighbour"));
    }

    /* ============================ fixtures ============================= */

    private void seedAllTables(String cx) {
        audit.insert(auditRow(cx, "killOp", Outcome.OK, 1_000L));
        audit.insert(auditRow(cx, "replSetStepDown", Outcome.FAIL, 2_000L));
        topology.insertIfChanged(cx, new TopologySnapshot(
                ClusterKind.REPLSET, 1_000L, "7.0.5", List.of(), List.of(),
                List.of(), List.of(), List.of()));
        roleCache.upsert(cx, new RoleSet(Set.of("root")), System.currentTimeMillis());
    }

    private static OpsAuditRecord auditRow(String cx, String cmd, Outcome outcome, long ts) {
        return new OpsAuditRecord(
                -1L, cx, null, null, cmd, "{\"" + cmd + "\":1}",
                "h".repeat(64), outcome, "msg", "root",
                ts, ts + 10, 10L,
                "localhost", "dba", "cluster.topology", false, false);
    }

    private static MongoConnection conn(String id, String name) {
        return new MongoConnection(
                id, name, "URI", "mongodb://localhost:27017",
                "STANDALONE", "localhost:27017", "",
                "NONE", "", null, "admin", "", "",
                false, "", "", null, false, false,
                false, "", 22, "", "PASSWORD", null, "", null,
                "NONE", "", 1080, "", null,
                "", "primary", "", "MongoExplorer", "",
                1_000L, 1_000L);
    }
}
