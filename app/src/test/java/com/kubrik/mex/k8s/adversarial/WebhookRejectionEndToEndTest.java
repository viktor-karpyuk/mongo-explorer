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
import com.kubrik.mex.k8s.rollout.DiagnosisEngine;
import com.kubrik.mex.k8s.rollout.ResourceCatalogue;
import com.kubrik.mex.k8s.rollout.RolloutEvent;
import com.kubrik.mex.k8s.rollout.RolloutEventDao;
import com.kubrik.mex.store.Database;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.1 Q2.8-L1 — Adversarial: an admission webhook rejects the CR
 * during apply; the resulting failure must preserve a hint the user
 * can act on rather than a raw SDK dump.
 *
 * <p>Unit {@link WebhookRejectionTest} already covers the diagnosis
 * engine's pattern matching. This IT closes the loop end to end:
 * the opener throws an {@link ApiException} with the real API server
 * webhook-denied message, and the orchestrator writes a failure row
 * whose {@code errorMessage} still contains the signal the
 * {@link DiagnosisEngine} needs to render a hint.</p>
 */
class WebhookRejectionEndToEndTest {

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
    void webhook_rejection_preserves_a_diagnosable_message() throws Exception {
        String webhookMessage =
                "admission webhook \"policy.kyverno.io\" denied the request: "
              + "validation error: require tls.enabled=true";

        ApplyOrchestrator.ApplyOpener opener = new ApplyOrchestrator.ApplyOpener() {
            @Override public void apply(ApiClient c, ResourceCatalogue.Ref r, String y) throws Exception {
                throw new ApiException(400, webhookMessage);
            }
            @Override public void delete(ApiClient c, ResourceCatalogue.Ref r) { /* unused */ }
        };
        ApplyOrchestrator orch = new ApplyOrchestrator(
                new Stub(), recordDao, eventDao, new EventBus(), opener);

        ProvisionModel m = new ProfileEnforcer().switchProfile(
                ProvisionModel.defaults(clusterId, "mongo", "prod-rs"),
                Profile.PROD).model()
                .withTls(TlsSpec.certManager("mongo-issuer"))
                .withStorage(new StorageSpec(Optional.of("gp3"), 200, 20));

        ApplyOrchestrator.ApplyResult r = orch.apply(refOf(), new McoAdapter(), m);
        assertFalse(r.ok(), "a webhook-denied apply must not succeed");

        ProvisioningRecord rec = recordDao.findById(r.provisioningId()).orElseThrow();
        assertEquals(ProvisioningRecord.Status.FAILED, rec.status());

        String err = r.error().orElse("");
        assertNotNull(err);
        assertTrue(err.toLowerCase().contains("admission webhook"),
                "preserved error should carry the webhook signal; got: " + err);

        // The DiagnosisEngine must still be able to produce a readable
        // hint when handed this message verbatim.
        Optional<String> hint = new DiagnosisEngine().diagnose(
                RolloutEvent.Source.CR_STATUS, "Invalid", err);
        assertTrue(hint.isPresent(), "diagnosis engine should match the webhook message");
        assertTrue(hint.get().toLowerCase().contains("webhook"));
    }

    private K8sClusterRef refOf() {
        return new K8sClusterRef(clusterId, "t", "/k", "ctx",
                Optional.empty(), Optional.empty(), 0L, Optional.empty());
    }

    static final class Stub extends KubeClientFactory {
        @Override public ApiClient get(K8sClusterRef ref) { return null; }
    }
}
