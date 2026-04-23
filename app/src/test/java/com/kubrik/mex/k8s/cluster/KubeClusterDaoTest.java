package com.kubrik.mex.k8s.cluster;

import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class KubeClusterDaoTest {

    @TempDir Path dataDir;

    private Database db;
    private KubeClusterDao dao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        dao = new KubeClusterDao(db);
    }

    @AfterEach
    void tearDown() throws Exception { if (db != null) db.close(); }

    @Test
    void round_trip_insert_then_find_then_list() throws SQLException {
        long id = dao.insert("dev-cluster", "/Users/me/.kube/config", "dev-ctx",
                Optional.of("apps"), Optional.of("https://dev:6443"));
        assertTrue(id > 0);

        K8sClusterRef got = dao.findById(id).orElseThrow();
        assertEquals("dev-cluster", got.displayName());
        assertEquals("dev-ctx", got.contextName());
        assertEquals("apps", got.defaultNamespace().orElse(null));
        assertEquals(Optional.of("https://dev:6443"), got.serverUrl());
        assertTrue(got.lastUsedAt().isEmpty());

        List<K8sClusterRef> all = dao.listAll();
        assertEquals(1, all.size());
    }

    @Test
    void unique_constraint_rejects_same_kubeconfig_and_context() throws SQLException {
        dao.insert("A", "/k1", "ctx-a", Optional.empty(), Optional.empty());
        SQLException sqle = assertThrows(SQLException.class, () ->
                dao.insert("A'", "/k1", "ctx-a", Optional.empty(), Optional.empty()));
        assertNotNull(sqle.getMessage());
    }

    @Test
    void touch_bumps_last_used_at() throws Exception {
        long id = dao.insert("A", "/k1", "ctx-a", Optional.empty(), Optional.empty());
        assertTrue(dao.findById(id).orElseThrow().lastUsedAt().isEmpty());
        dao.touch(id);
        assertTrue(dao.findById(id).orElseThrow().lastUsedAt().isPresent());
    }

    @Test
    void delete_removes_row() throws SQLException {
        long id = dao.insert("A", "/k1", "ctx-a", Optional.empty(), Optional.empty());
        dao.delete(id);
        assertTrue(dao.findById(id).isEmpty());
    }

    @Test
    void counts_live_provisions_zero_when_none() throws SQLException {
        long id = dao.insert("A", "/k1", "ctx-a", Optional.empty(), Optional.empty());
        assertEquals(0, dao.countLiveProvisions(id));
    }

    @Test
    void rename_and_default_namespace_update() throws SQLException {
        long id = dao.insert("A", "/k1", "ctx-a", Optional.empty(), Optional.empty());
        dao.rename(id, "prod-east");
        dao.updateDefaultNamespace(id, Optional.of("mongo-prod"));
        K8sClusterRef got = dao.findById(id).orElseThrow();
        assertEquals("prod-east", got.displayName());
        assertEquals("mongo-prod", got.defaultNamespace().orElse(null));
        dao.updateDefaultNamespace(id, Optional.empty());
        assertTrue(dao.findById(id).orElseThrow().defaultNamespace().isEmpty());
    }
}
