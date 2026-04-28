package com.kubrik.mex.labs.k8s.store;

import com.kubrik.mex.labs.k8s.model.LabK8sCluster;
import com.kubrik.mex.labs.k8s.model.LabK8sClusterStatus;
import com.kubrik.mex.labs.k8s.model.LabK8sDistro;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

class LabK8sClusterDaoTest {

    @TempDir Path tmp;

    private Database db;
    private LabK8sClusterDao dao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", tmp.toString());
        db = new Database();
        dao = new LabK8sClusterDao(db);
    }

    @AfterEach
    void tearDown() throws Exception { if (db != null) db.close(); }

    @Test
    void insert_creating_then_read_back() throws SQLException {
        long id = dao.insertCreating(LabK8sDistro.MINIKUBE, "dev-lab",
                "dev-lab", "/home/me/.kube/config");
        assertTrue(id > 0);
        LabK8sCluster row = dao.findById(id).orElseThrow();
        assertEquals(LabK8sDistro.MINIKUBE, row.distro());
        assertEquals("dev-lab", row.identifier());
        assertEquals(LabK8sClusterStatus.CREATING, row.status());
        assertTrue(row.lastStartedAt().isEmpty());
    }

    @Test
    void find_by_identifier_resolves_distro_scoped() throws SQLException {
        dao.insertCreating(LabK8sDistro.MINIKUBE, "shared", "shared", "/k");
        dao.insertCreating(LabK8sDistro.K3D, "shared", "k3d-shared", "/k");

        assertTrue(dao.findByIdentifier(LabK8sDistro.MINIKUBE, "shared").isPresent());
        assertEquals(LabK8sDistro.K3D,
                dao.findByIdentifier(LabK8sDistro.K3D, "shared").orElseThrow().distro());
    }

    @Test
    void update_status_stamps_the_chosen_timestamp_column() throws SQLException {
        long id = dao.insertCreating(LabK8sDistro.MINIKUBE, "x", "x", "/k");
        dao.updateStatus(id, LabK8sClusterStatus.RUNNING,
                1_700_000_000_000L, "last_started_at");
        LabK8sCluster after = dao.findById(id).orElseThrow();
        assertEquals(LabK8sClusterStatus.RUNNING, after.status());
        assertEquals(1_700_000_000_000L, after.lastStartedAt().orElseThrow());
        assertTrue(after.lastStoppedAt().isEmpty());
    }

    @Test
    void update_status_rejects_arbitrary_column_names() throws SQLException {
        long id = dao.insertCreating(LabK8sDistro.MINIKUBE, "x", "x", "/k");
        // Guard against SQL injection via the timestamp-column arg.
        assertThrows(IllegalArgumentException.class, () ->
                dao.updateStatus(id, LabK8sClusterStatus.RUNNING, 0L,
                        "status; DROP TABLE lab_k8s_clusters"));
    }

    @Test
    void live_list_excludes_destroyed() throws SQLException {
        long a = dao.insertCreating(LabK8sDistro.MINIKUBE, "alive", "alive", "/k");
        long b = dao.insertCreating(LabK8sDistro.MINIKUBE, "gone", "gone", "/k");
        dao.updateStatus(b, LabK8sClusterStatus.DESTROYED, 0L, "destroyed_at");

        assertEquals(1, dao.listLive().size());
        assertEquals(2, dao.listAll().size());
    }

    @Test
    void unique_constraint_on_distro_plus_identifier() throws SQLException {
        dao.insertCreating(LabK8sDistro.MINIKUBE, "dup", "dup", "/k");
        assertThrows(SQLException.class, () ->
                dao.insertCreating(LabK8sDistro.MINIKUBE, "dup", "dup2", "/k"));
    }
}
