package com.kubrik.mex.k8s.apply;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.cluster.KubeClusterDao;
import com.kubrik.mex.k8s.events.ProvisionEvent;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.model.ProvisioningRecord;
import com.kubrik.mex.k8s.operator.mco.McoAdapter;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.k8s.rollout.ResourceCatalogue;
import com.kubrik.mex.k8s.rollout.RolloutEventDao;
import com.kubrik.mex.store.Database;
import io.kubernetes.client.openapi.ApiClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class ApplyOrchestratorTest {

    @TempDir Path dataDir;

    private Database db;
    private EventBus bus;
    private ProvisioningRecordDao recordDao;
    private RolloutEventDao eventDao;
    private long clusterId;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        bus = new EventBus();
        recordDao = new ProvisioningRecordDao(db);
        eventDao = new RolloutEventDao(db);
        clusterId = new KubeClusterDao(db).insert(
                "t", "/k", "ctx", Optional.empty(), Optional.empty());
    }

    @AfterEach
    void tearDown() throws Exception { if (db != null) db.close(); }

    @Test
    void apply_inserts_provisioning_record_and_emits_progress() throws Exception {
        Recorder opener = new Recorder();
        ApplyOrchestrator orch = new ApplyOrchestrator(
                new StubFactory(), recordDao, eventDao, bus, opener);

        List<ProvisionEvent> seen = new CopyOnWriteArrayList<>();
        bus.onProvision(seen::add);

        ProvisionModel m = ProvisionModel.defaults(clusterId, "mongo", "dev-rs");
        ApplyOrchestrator.ApplyResult result = orch.apply(ref(), new McoAdapter(), m);

        assertTrue(result.ok(), "recorder opener never throws → apply is ok");
        assertTrue(opener.calls.size() >= 1);
        assertTrue(seen.stream().anyMatch(e -> e instanceof ProvisionEvent.Started));
        assertTrue(seen.stream().anyMatch(e -> e instanceof ProvisionEvent.Progress));

        ProvisioningRecord row = recordDao.findById(result.provisioningId()).orElseThrow();
        assertEquals(ProvisioningRecord.Status.APPLYING, row.status());
        assertEquals("MCO", row.operator());
        assertFalse(row.crSha256().isBlank(), "sha256 must be persisted");
    }

    @Test
    void mark_ready_updates_status_and_publishes_event() throws Exception {
        ApplyOrchestrator orch = new ApplyOrchestrator(
                new StubFactory(), recordDao, eventDao, bus, new Recorder());
        ProvisionModel m = ProvisionModel.defaults(clusterId, "mongo", "dev-rs");
        long rowId = orch.apply(ref(), new McoAdapter(), m).provisioningId();

        List<ProvisionEvent> seen = new CopyOnWriteArrayList<>();
        bus.onProvision(seen::add);

        orch.markReady(rowId, "dev-rs");
        ProvisioningRecord row = recordDao.findById(rowId).orElseThrow();
        assertEquals(ProvisioningRecord.Status.READY, row.status());
        assertTrue(row.appliedAt().isPresent());
        assertTrue(seen.stream().anyMatch(e -> e instanceof ProvisionEvent.Ready));
    }

    @Test
    void failed_apply_records_catalogue_for_cleanup() throws Exception {
        FailingOpener opener = new FailingOpener(2);  // fail on 3rd call
        ApplyOrchestrator orch = new ApplyOrchestrator(
                new StubFactory(), recordDao, eventDao, bus, opener);

        // Prod model renders CR + PDB + ServiceMonitor + (PBM bundle …)
        // so we cross the "fail on 3rd apply" threshold.
        com.kubrik.mex.k8s.provision.ProfileEnforcer enforcer =
                new com.kubrik.mex.k8s.provision.ProfileEnforcer();
        ProvisionModel prod = enforcer.switchProfile(
                ProvisionModel.defaults(clusterId, "mongo", "prod"),
                com.kubrik.mex.k8s.provision.Profile.PROD).model()
                .withTls(com.kubrik.mex.k8s.provision.TlsSpec.certManager("mongo-issuer"))
                .withStorage(new com.kubrik.mex.k8s.provision.StorageSpec(
                        Optional.of("gp3"), 200, 20));

        ApplyOrchestrator.ApplyResult r = orch.apply(ref(), new McoAdapter(), prod);

        assertFalse(r.ok());
        assertEquals(2, r.catalogue().size(),
                "catalogue must carry every successful apply so cleanup can reverse-iterate");
        ProvisioningRecord row = recordDao.findById(r.provisioningId()).orElseThrow();
        assertEquals(ProvisioningRecord.Status.FAILED, row.status());
    }

    @Test
    void sha256_is_stable_for_same_model() throws Exception {
        ApplyOrchestrator orch = new ApplyOrchestrator(
                new StubFactory(), recordDao, eventDao, bus, new Recorder());
        ProvisionModel m = ProvisionModel.defaults(clusterId, "mongo", "stable");
        String first = orch.apply(ref(), new McoAdapter(), m).sha256();
        // Drop the first row (unique constraint on (cluster, ns, name)) — just clear the table.
        db.connection().createStatement().execute("DELETE FROM provisioning_records");
        String second = orch.apply(ref(), new McoAdapter(), m).sha256();
        assertEquals(first, second, "same model → same manifest bytes → same SHA-256");
    }

    private K8sClusterRef ref() {
        return new K8sClusterRef(clusterId, "t", "/k", "ctx",
                Optional.empty(), Optional.empty(), 0L, Optional.empty());
    }

    /* ============================ fixtures ============================ */

    static final class StubFactory extends KubeClientFactory {
        @Override public ApiClient get(K8sClusterRef ref) { return null; }
    }

    static final class Recorder implements ApplyOrchestrator.ApplyOpener {
        final List<ResourceCatalogue.Ref> calls = new ArrayList<>();
        @Override public void apply(ApiClient c, ResourceCatalogue.Ref r, String yaml) { calls.add(r); }
        @Override public void delete(ApiClient c, ResourceCatalogue.Ref r) { calls.remove(r); }
    }

    static final class FailingOpener implements ApplyOrchestrator.ApplyOpener {
        private int successesAllowed;
        FailingOpener(int n) { this.successesAllowed = n; }
        @Override public void apply(ApiClient c, ResourceCatalogue.Ref r, String yaml) throws Exception {
            if (successesAllowed-- <= 0) throw new RuntimeException("synthetic apply failure");
        }
        @Override public void delete(ApiClient c, ResourceCatalogue.Ref r) {}
    }
}
