package com.kubrik.mex.k8s.portforward;

import com.kubrik.mex.k8s.model.PortForwardTarget;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

class PortForwardAuditDaoTest {

    @TempDir Path dataDir;

    private Database db;
    private PortForwardAuditDao dao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        dao = new PortForwardAuditDao(db);
    }

    @AfterEach
    void tearDown() throws Exception { if (db != null) db.close(); }

    @Test
    void open_insert_returns_positive_id_and_sets_columns() throws SQLException {
        long id = dao.insertOpen("conn-1", null,
                PortForwardTarget.forService("mongo", "mongo-svc", 27017),
                31000, 1_700_000_000_000L);
        assertTrue(id > 0);

        try (ResultSet rs = db.connection().createStatement()
                .executeQuery("SELECT target_kind, target_name, remote_port, local_port, "
                        + "closed_at FROM portforward_audit WHERE id = " + id)) {
            assertTrue(rs.next());
            assertEquals("SERVICE", rs.getString("target_kind"));
            assertEquals("mongo-svc", rs.getString("target_name"));
            assertEquals(27017, rs.getInt("remote_port"));
            assertEquals(31000, rs.getInt("local_port"));
            rs.getLong("closed_at");
            assertTrue(rs.wasNull(), "closed_at must be null on open row");
        }
    }

    @Test
    void pod_kind_stored_for_pod_target() throws SQLException {
        long id = dao.insertOpen("conn-2", null,
                PortForwardTarget.forPod("mongo", "mongo-0", 27017),
                31001, 0L);
        try (ResultSet rs = db.connection().createStatement()
                .executeQuery("SELECT target_kind, target_name FROM portforward_audit WHERE id = " + id)) {
            assertTrue(rs.next());
            assertEquals("POD", rs.getString("target_kind"));
            assertEquals("mongo-0", rs.getString("target_name"));
        }
    }

    @Test
    void mark_closed_flips_row() throws SQLException {
        long id = dao.insertOpen("conn-3", null,
                PortForwardTarget.forService("mongo", "svc", 27017), 31002, 0L);
        dao.markClosed(id, 1_700_000_005_000L, "MANUAL");
        try (ResultSet rs = db.connection().createStatement()
                .executeQuery("SELECT closed_at, reason_closed FROM portforward_audit WHERE id = " + id)) {
            assertTrue(rs.next());
            assertEquals(1_700_000_005_000L, rs.getLong("closed_at"));
            assertEquals("MANUAL", rs.getString("reason_closed"));
        }
    }

    @Test
    void mark_closed_is_idempotent_on_already_closed_row() throws SQLException {
        long id = dao.insertOpen("c", null,
                PortForwardTarget.forService("mongo", "svc", 27017), 31003, 0L);
        dao.markClosed(id, 100L, "MANUAL");
        dao.markClosed(id, 200L, "DIFFERENT");
        try (ResultSet rs = db.connection().createStatement()
                .executeQuery("SELECT closed_at, reason_closed FROM portforward_audit WHERE id = " + id)) {
            assertTrue(rs.next());
            assertEquals(100L, rs.getLong("closed_at"), "first-write-wins: second close is ignored");
            assertEquals("MANUAL", rs.getString("reason_closed"));
        }
    }

    @Test
    void count_open_excludes_closed_rows() throws SQLException {
        long a = dao.insertOpen("c", null,
                PortForwardTarget.forService("mongo", "svc", 27017), 31004, 0L);
        dao.insertOpen("c", null,
                PortForwardTarget.forService("mongo", "svc", 27017), 31005, 0L);
        assertEquals(2, dao.countOpen("c"));
        dao.markClosed(a, 1L, "MANUAL");
        assertEquals(1, dao.countOpen("c"));
    }
}
