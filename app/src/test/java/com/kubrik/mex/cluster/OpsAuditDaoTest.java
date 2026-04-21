package com.kubrik.mex.cluster;

import com.kubrik.mex.cluster.audit.OpsAuditRecord;
import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.store.OpsAuditDao;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.4 AUD-1..3 — schema round-trip for {@code ops_audit}. Every persisted row
 * carries a non-null {@code preview_hash}, {@code ui_source}, and {@code outcome};
 * paste + kill_switch booleans survive storage; cascade-delete purges rows for a
 * connection without touching neighbours.
 */
class OpsAuditDaoTest {

    @TempDir Path dataDir;

    private Database db;
    private OpsAuditDao dao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        dao = new OpsAuditDao(db);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void round_trips_every_column() {
        OpsAuditRecord in = new OpsAuditRecord(
                -1L, "cx-1", "admin", null,
                "replSetStepDown",
                "{\"replSetStepDown\":60,\"secondaryCatchUpPeriodSecs\":10}",
                "deadbeef".repeat(8),
                Outcome.OK, "ok", "clusterManager",
                1_000L, 1_250L, 250L,
                "host.local", "dba", "cluster.topology",
                true, false);

        OpsAuditRecord saved = dao.insert(in);
        assertTrue(saved.id() > 0, "id assigned on insert");

        OpsAuditRecord back = dao.byId(saved.id());
        assertNotNull(back);
        assertEquals("cx-1", back.connectionId());
        assertEquals("admin", back.db());
        assertNull(back.coll());
        assertEquals("replSetStepDown", back.commandName());
        assertEquals(Outcome.OK, back.outcome());
        assertEquals(250L, back.latencyMs());
        assertTrue(back.paste());
        assertFalse(back.killSwitch());
        assertEquals(in.previewHash(), back.previewHash());
    }

    @Test
    void lists_newest_first_per_connection() {
        dao.insert(sample("cx-A", "a", 100L));
        dao.insert(sample("cx-A", "b", 200L));
        dao.insert(sample("cx-B", "c", 300L));

        List<OpsAuditRecord> a = dao.listForConnection("cx-A", 10);
        assertEquals(2, a.size());
        assertEquals("b", a.get(0).commandName());
        assertEquals("a", a.get(1).commandName());
    }

    @Test
    void cascade_delete_removes_only_target_connection() throws Exception {
        dao.insert(sample("cx-A", "a", 100L));
        dao.insert(sample("cx-B", "b", 200L));
        dao.deleteForConnection(db.connection(), "cx-A");
        assertEquals(0, dao.listForConnection("cx-A", 10).size());
        assertEquals(1, dao.listForConnection("cx-B", 10).size());
    }

    private static OpsAuditRecord sample(String cx, String cmd, long startedAt) {
        return new OpsAuditRecord(
                -1L, cx, null, null, cmd,
                "{\"" + cmd + "\":1}",
                "h".repeat(64),
                Outcome.OK, null, null,
                startedAt, null, null,
                null, null, "cluster.topology",
                false, false);
    }
}
