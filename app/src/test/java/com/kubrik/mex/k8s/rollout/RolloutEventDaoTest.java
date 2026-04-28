package com.kubrik.mex.k8s.rollout;

import com.kubrik.mex.k8s.cluster.KubeClusterDao;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class RolloutEventDaoTest {

    @TempDir Path dataDir;

    private Database db;
    private RolloutEventDao dao;
    private long provisioningId;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        dao = new RolloutEventDao(db);

        // rollout_events FKs provisioning_records; seed a row so inserts pass the FK check.
        long clusterId = new KubeClusterDao(db).insert(
                "t", "/k", "ctx", Optional.empty(), Optional.empty());
        try (PreparedStatement ps = db.connection().prepareStatement(
                "INSERT INTO provisioning_records(k8s_cluster_id, namespace, name, operator, "
                + "operator_version, mongo_version, topology, profile, cr_yaml, cr_sha256, "
                + "deletion_protection, created_at, status) "
                + "VALUES (?, ?, ?, 'MCO', 'x', '7.0', 'RS3', 'DEV_TEST', '', '', 0, 0, 'APPLYING')",
                java.sql.Statement.RETURN_GENERATED_KEYS)) {
            ps.setLong(1, clusterId);
            ps.setString(2, "mongo");
            ps.setString(3, "rs");
            ps.executeUpdate();
            try (var rs = ps.getGeneratedKeys()) {
                rs.next();
                provisioningId = rs.getLong(1);
            }
        }
    }

    @AfterEach
    void tearDown() throws Exception { if (db != null) db.close(); }

    @Test
    void inserts_and_lists_back_ordered_by_at() throws SQLException {
        dao.insert(evt(100, "a"));
        dao.insert(evt(200, "b"));
        dao.insert(evt(50, "zzz"));
        List<RolloutEvent> list = dao.listForProvisioning(provisioningId);
        assertEquals(3, list.size());
        assertEquals(50L, list.get(0).at());
        assertEquals(100L, list.get(1).at());
        assertEquals(200L, list.get(2).at());
    }

    @Test
    void diagnosis_hint_is_preserved_on_read_back() throws SQLException {
        RolloutEvent withHint = new RolloutEvent(provisioningId, 1L,
                RolloutEvent.Source.POD, RolloutEvent.Severity.ERROR,
                Optional.of("ImagePullBackOff"),
                Optional.of("back-off pulling"),
                Optional.of("Image pull failed."));
        dao.insert(withHint);
        List<RolloutEvent> list = dao.listForProvisioning(provisioningId);
        assertEquals("Image pull failed.", list.get(0).diagnosisHint().orElseThrow());
    }

    @Test
    void null_reason_and_message_are_stored_as_null() throws SQLException {
        dao.insert(new RolloutEvent(provisioningId, 1L,
                RolloutEvent.Source.APPLY, RolloutEvent.Severity.INFO,
                Optional.empty(), Optional.empty(), Optional.empty()));
        RolloutEvent read = dao.listForProvisioning(provisioningId).get(0);
        assertTrue(read.reason().isEmpty());
        assertTrue(read.message().isEmpty());
    }

    private RolloutEvent evt(long at, String msg) {
        return new RolloutEvent(provisioningId, at,
                RolloutEvent.Source.POD, RolloutEvent.Severity.INFO,
                Optional.of("test"), Optional.of(msg), Optional.empty());
    }
}
