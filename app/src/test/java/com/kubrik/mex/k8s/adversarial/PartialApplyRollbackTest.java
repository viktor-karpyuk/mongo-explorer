package com.kubrik.mex.k8s.adversarial;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.k8s.apply.ApplyOrchestrator;
import com.kubrik.mex.k8s.apply.ProvisioningRecordDao;
import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.cluster.KubeClusterDao;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.model.ProvisioningRecord;
import com.kubrik.mex.k8s.operator.mco.McoAdapter;
import com.kubrik.mex.k8s.provision.Profile;
import com.kubrik.mex.k8s.provision.ProfileEnforcer;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.k8s.provision.StorageSpec;
import com.kubrik.mex.k8s.provision.TlsSpec;
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

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.1 Q2.8.1-L — Adversarial: a partial apply must leave the
 * catalogue complete enough for cleanup to reverse-iterate every
 * successfully applied document.
 *
 * <p>Fires the renderer for a Prod MCO model (multiple aux docs),
 * fails on the third apply call, and verifies the cleanup path
 * receives exactly the two earlier objects in reverse order.</p>
 */
class PartialApplyRollbackTest {

    @TempDir Path dataDir;

    private Database db;
    private ProvisioningRecordDao recordDao;
    private RolloutEventDao eventDao;
    private long clusterId;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        recordDao = new ProvisioningRecordDao(db);
        eventDao = new RolloutEventDao(db);
        clusterId = new KubeClusterDao(db).insert(
                "t", "/k", "ctx", Optional.empty(), Optional.empty());
    }

    @AfterEach
    void tearDown() throws Exception { if (db != null) db.close(); }

    @Test
    void failed_apply_leaves_catalogue_for_reverse_cleanup() throws Exception {
        RecordingOpener opener = new RecordingOpener(2);
        EventBus bus = new EventBus();
        ApplyOrchestrator orch = new ApplyOrchestrator(
                new Stub(), recordDao, eventDao, bus, opener);

        ProvisionModel m = new ProfileEnforcer().switchProfile(
                ProvisionModel.defaults(clusterId, "mongo", "prod-rs"),
                Profile.PROD).model()
                .withTls(TlsSpec.certManager("mongo-issuer"))
                .withStorage(new StorageSpec(Optional.of("gp3"), 200, 20));

        ApplyOrchestrator.ApplyResult r = orch.apply(refOf(), new McoAdapter(), m);
        assertFalse(r.ok());

        // Now cleanup — it should reverse-iterate the catalogue.
        orch.cleanup(refOf(), r.catalogue());
        // The two successful apply calls should now have matching
        // delete calls, but in reverse order.
        List<ResourceCatalogue.Ref> applyOrder = opener.applied;
        List<ResourceCatalogue.Ref> deleteOrder = opener.deleted;
        assertEquals(2, applyOrder.size());
        assertEquals(2, deleteOrder.size());
        assertEquals(applyOrder.get(0), deleteOrder.get(1),
                "cleanup must visit in reverse order");
        assertEquals(applyOrder.get(1), deleteOrder.get(0),
                "cleanup must visit in reverse order");

        assertEquals(ProvisioningRecord.Status.FAILED,
                recordDao.findById(r.provisioningId()).orElseThrow().status());
    }

    private K8sClusterRef refOf() {
        return new K8sClusterRef(clusterId, "t", "/k", "ctx",
                Optional.empty(), Optional.empty(), 0L, Optional.empty());
    }

    static final class Stub extends KubeClientFactory {
        @Override public ApiClient get(K8sClusterRef ref) { return null; }
    }

    /** Counts applies; throws on the (n+1)-th so the partial-apply is deterministic. */
    static final class RecordingOpener implements ApplyOrchestrator.ApplyOpener {
        private int successesLeft;
        final List<ResourceCatalogue.Ref> applied = new ArrayList<>();
        final List<ResourceCatalogue.Ref> deleted = new ArrayList<>();

        RecordingOpener(int successesAllowed) { this.successesLeft = successesAllowed; }

        @Override
        public void apply(ApiClient c, ResourceCatalogue.Ref r, String yaml) throws Exception {
            if (successesLeft-- <= 0) throw new RuntimeException("forced failure on step " + r.kind());
            applied.add(r);
        }

        @Override
        public void delete(ApiClient c, ResourceCatalogue.Ref r) {
            deleted.add(r);
        }
    }
}
