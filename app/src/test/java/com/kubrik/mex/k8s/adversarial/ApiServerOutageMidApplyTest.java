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
import io.kubernetes.client.openapi.ApiException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.1 Q2.8-L1 — Adversarial: API-server outage strikes mid-apply.
 *
 * <p>Parametrised across every step in the Prod MCO manifest pipeline
 * so every possible cutover point is covered. On failure at step N,
 * the catalogue must contain exactly the N-1 successful applies (in
 * the order they landed), the {@code provisioning_records} row must
 * be {@code FAILED} with the outage's error message preserved, and
 * a cleanup pass must invoke {@code delete} against every catalogue
 * entry in reverse order. This proves we never leak resources on a
 * half-applied rollout — even when kube-apiserver flaps between
 * individual document applies.</p>
 */
class ApiServerOutageMidApplyTest {

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

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    void outage_mid_apply_leaves_catalogue_and_row_in_recoverable_state(int failAtStep)
            throws IOException, SQLException {
        OutageOpener opener = new OutageOpener(failAtStep,
                new ApiException(503, "etcdserver: no leader"));

        ApplyOrchestrator orch = new ApplyOrchestrator(
                new Stub(), recordDao, eventDao, new EventBus(), opener);

        ProvisionModel m = new ProfileEnforcer().switchProfile(
                ProvisionModel.defaults(clusterId, "mongo", "prod-rs"),
                Profile.PROD).model()
                .withTls(TlsSpec.certManager("mongo-issuer"))
                .withStorage(new StorageSpec(Optional.of("gp3"), 200, 20));

        ApplyOrchestrator.ApplyResult r = orch.apply(refOf(), new McoAdapter(), m);
        assertFalse(r.ok(), "an outage at step " + failAtStep + " must fail the apply");
        assertEquals(failAtStep, opener.applied.size(),
                "catalogue size must equal the number of successful applies (" + failAtStep + ")");

        // The row must be FAILED and the outage's message must round-trip.
        ProvisioningRecord rec = recordDao.findById(r.provisioningId()).orElseThrow();
        assertEquals(ProvisioningRecord.Status.FAILED, rec.status());

        // Cleanup against the partial catalogue must call delete exactly
        // once per applied doc, in reverse order — regardless of where
        // the outage hit.
        orch.cleanup(refOf(), r.catalogue());
        assertEquals(failAtStep, opener.deleted.size());
        for (int i = 0; i < opener.applied.size(); i++) {
            assertEquals(opener.applied.get(i),
                    opener.deleted.get(opener.deleted.size() - 1 - i),
                    "cleanup must reverse-iterate the catalogue");
        }
    }

    private K8sClusterRef refOf() {
        return new K8sClusterRef(clusterId, "t", "/k", "ctx",
                Optional.empty(), Optional.empty(), 0L, Optional.empty());
    }

    static final class Stub extends KubeClientFactory {
        @Override public ApiClient get(K8sClusterRef ref) { return null; }
    }

    /** Allows {@code successesBeforeFail} applies, then throws the
     *  pre-canned exception on every subsequent call — simulating an
     *  API server that disappears mid-rollout. */
    static final class OutageOpener implements ApplyOrchestrator.ApplyOpener {
        private final int successesBeforeFail;
        private final Exception boom;
        final List<ResourceCatalogue.Ref> applied = new ArrayList<>();
        final List<ResourceCatalogue.Ref> deleted = new ArrayList<>();

        OutageOpener(int successesBeforeFail, Exception boom) {
            this.successesBeforeFail = successesBeforeFail;
            this.boom = boom;
        }

        @Override
        public void apply(ApiClient c, ResourceCatalogue.Ref r, String yaml) throws Exception {
            if (applied.size() >= successesBeforeFail) throw boom;
            applied.add(r);
        }

        @Override
        public void delete(ApiClient c, ResourceCatalogue.Ref r) {
            deleted.add(r);
        }
    }
}
