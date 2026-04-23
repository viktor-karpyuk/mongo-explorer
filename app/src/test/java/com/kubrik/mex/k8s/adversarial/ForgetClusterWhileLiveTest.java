package com.kubrik.mex.k8s.adversarial;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.k8s.apply.ProvisioningRecordDao;
import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.cluster.ClusterProbeService;
import com.kubrik.mex.k8s.cluster.KubeClusterDao;
import com.kubrik.mex.k8s.cluster.KubeClusterService;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.1 Q2.8.1-L — Adversarial: forgetting a cluster while a
 * provisioning row points at it must bounce with a friendly
 * message (not the raw SQLite FK error).
 */
class ForgetClusterWhileLiveTest {

    @TempDir Path dataDir;

    private Database db;
    private KubeClusterDao clusterDao;
    private ProvisioningRecordDao recordDao;
    private KubeClusterService service;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        clusterDao = new KubeClusterDao(db);
        recordDao = new ProvisioningRecordDao(db);
        KubeClientFactory factory = new KubeClientFactory();
        service = new KubeClusterService(
                clusterDao, factory,
                new ClusterProbeService(factory),
                new EventBus());
    }

    @AfterEach
    void tearDown() throws Exception { if (db != null) db.close(); }

    @Test
    void forget_with_live_applying_provision_is_refused() throws Exception {
        long clusterId = clusterDao.insert("t", "/k", "ctx",
                Optional.empty(), Optional.empty());
        recordDao.insertApplying(clusterId, "mongo", "prod-rs",
                "MCO", "x", "7.0", "RS3", "PROD",
                "", "", true);

        IllegalStateException ise = assertThrows(IllegalStateException.class,
                () -> service.remove(clusterId));
        assertTrue(ise.getMessage().contains("refuse to forget"),
                "message should explain why: " + ise.getMessage());
        assertTrue(clusterDao.findById(clusterId).isPresent(),
                "cluster row must survive the refused delete");
    }

    @Test
    void forget_with_no_provisions_succeeds() throws Exception {
        long clusterId = clusterDao.insert("t", "/k", "ctx",
                Optional.empty(), Optional.empty());
        service.remove(clusterId);
        assertTrue(clusterDao.findById(clusterId).isEmpty());
    }
}
