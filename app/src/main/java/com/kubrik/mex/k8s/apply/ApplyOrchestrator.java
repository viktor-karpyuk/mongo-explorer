package com.kubrik.mex.k8s.apply;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.events.ProvisionEvent;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.model.ProvisioningRecord;
import com.kubrik.mex.k8s.operator.KubernetesManifests;
import com.kubrik.mex.k8s.operator.OperatorAdapter;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.k8s.rollout.ResourceCatalogue;
import com.kubrik.mex.k8s.rollout.RolloutEvent;
import com.kubrik.mex.k8s.rollout.RolloutEventDao;
import io.kubernetes.client.openapi.ApiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Objects;

/**
 * v2.8.1 Q2.8.1-H — Top-level Apply path.
 *
 * <p>Flow:</p>
 * <ol>
 *   <li>Render the manifests via the operator adapter.</li>
 *   <li>Compute the SHA-256 of every manifest concatenated in
 *       order; persist to the {@code provisioning_records} row so
 *       {@code PreviewHashChecker} (v2.4) can verify the preview
 *       didn't drift.</li>
 *   <li>Insert row in {@code APPLYING}; publish
 *       {@link ProvisionEvent.Started}.</li>
 *   <li>Apply manifests in the renderer's emission order. Each
 *       applied object is recorded in {@link ResourceCatalogue} so
 *       cleanup can reverse-iterate.</li>
 *   <li>Publish {@link ProvisionEvent.Progress} per step + write
 *       a synthetic {@link RolloutEvent} per step.</li>
 *   <li>Caller drives the status watch loop separately (future
 *       follow-up chunk); for now Apply returns after the last
 *       document is created and the caller calls
 *       {@link #markReady} / {@link #markFailed} when the operator's
 *       status parser lands on READY / FAILED.</li>
 * </ol>
 *
 * <p>This chunk lands the orchestration + persistence + event
 * wiring. Live YAML-to-apply-call dispatch against a real cluster
 * is a function of the opener seam {@link ApplyOpener}; Q2.8.1-L's
 * kind IT plugs in the real dispatcher, unit tests plug a recorder.</p>
 */
public final class ApplyOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(ApplyOrchestrator.class);

    private final KubeClientFactory clientFactory;
    private final ProvisioningRecordDao recordDao;
    private final RolloutEventDao eventDao;
    private final EventBus events;
    private final ApplyOpener opener;

    public ApplyOrchestrator(KubeClientFactory clientFactory,
                              ProvisioningRecordDao recordDao,
                              RolloutEventDao eventDao,
                              EventBus events) {
        // Production path uses the live API-server dispatcher. The
        // orchestrator hands an ApiClient per call; the adapter
        // constructs a short-lived LiveApplyOpener per step so the
        // DynamicKubernetesApi wrappers cache against the right
        // client without sharing state across clusters.
        this(clientFactory, recordDao, eventDao, events, new LiveDispatcher());
    }

    /**
     * Adapter that builds a {@link LiveApplyOpener} per call using
     * the {@link ApiClient} the orchestrator resolves from the
     * factory for the current cluster ref.
     */
    private static final class LiveDispatcher implements ApplyOpener {
        @Override
        public void apply(ApiClient client,
                          com.kubrik.mex.k8s.rollout.ResourceCatalogue.Ref ref,
                          String yaml) throws Exception {
            new LiveApplyOpener(client).apply(client, ref, yaml);
        }
        @Override
        public void delete(ApiClient client,
                           com.kubrik.mex.k8s.rollout.ResourceCatalogue.Ref ref) throws Exception {
            new LiveApplyOpener(client).delete(client, ref);
        }
    }

    /** 5-arg constructor — exposed for tests + Q2.8.1-L adversarial suites. */
    public ApplyOrchestrator(KubeClientFactory clientFactory,
                       ProvisioningRecordDao recordDao,
                       RolloutEventDao eventDao,
                       EventBus events,
                       ApplyOpener opener) {
        this.clientFactory = Objects.requireNonNull(clientFactory);
        this.recordDao = Objects.requireNonNull(recordDao);
        this.eventDao = Objects.requireNonNull(eventDao);
        this.events = Objects.requireNonNull(events);
        this.opener = Objects.requireNonNull(opener);
    }

    /** Run Apply. Returns the new {@code provisioning_records} row id on success. */
    public ApplyResult apply(K8sClusterRef clusterRef,
                               OperatorAdapter adapter,
                               ProvisionModel model) throws IOException {
        KubernetesManifests manifests = adapter.render(model);
        String sha256 = manifestSha256(manifests);
        long rowId;
        try {
            rowId = recordDao.insertApplying(
                    clusterRef.id(), model.namespace(), model.deploymentName(),
                    adapter.id().name(), "unknown",
                    model.mongoVersion(), model.topology().name(),
                    model.profile().name(), manifests.crYaml(), sha256,
                    model.profile() == com.kubrik.mex.k8s.provision.Profile.PROD);
        } catch (Exception e) {
            throw new IOException("insert provisioning row failed: " + e.getMessage(), e);
        }
        events.publishProvision(new ProvisionEvent.Started(
                rowId, model.deploymentName(), System.currentTimeMillis()));

        ApiClient client;
        try {
            client = clientFactory.get(clusterRef);
        } catch (IOException ioe) {
            recordFail(rowId, "kubeconfig build failed: " + ioe.getMessage());
            throw ioe;
        }

        ResourceCatalogue catalogue = new ResourceCatalogue();
        try {
            for (KubernetesManifests.Manifest doc : manifests.documents()) {
                ResourceCatalogue.Ref ref = new ResourceCatalogue.Ref(
                        apiVersionOf(doc.kind()), doc.kind(),
                        model.namespace(), doc.name());
                opener.apply(client, ref, doc.yaml());
                catalogue.record(ref);
                recordProgress(rowId, "applied " + doc.kind() + "/" + doc.name());
            }
            return new ApplyResult(rowId, catalogue, sha256);
        } catch (Exception e) {
            log.warn("apply failed at row {}: {}", rowId, e.toString());
            recordFail(rowId, e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
            // Hand the catalogue back so the caller can tear down.
            return new ApplyResult(rowId, catalogue, sha256, e.getMessage());
        }
    }

    public void markReady(long rowId, String deploymentName) throws java.sql.SQLException {
        recordDao.markStatus(rowId, ProvisioningRecord.Status.READY);
        events.publishProvision(new ProvisionEvent.Ready(
                rowId, deploymentName, System.currentTimeMillis()));
    }

    public void markFailed(long rowId, String reason) throws java.sql.SQLException {
        recordDao.markStatus(rowId, ProvisioningRecord.Status.FAILED);
        events.publishProvision(new ProvisionEvent.Failed(
                rowId, reason, System.currentTimeMillis()));
    }

    /**
     * v2.8.1 Q2.8.1-L — Apply + watch in one call.
     *
     * <p>Runs {@link #apply}, then polls the CR status via a
     * {@link com.kubrik.mex.k8s.rollout.RolloutWatcher} until it
     * lands on READY or FAILED. Each poll's status transition + any
     * poll errors are persisted as {@link
     * com.kubrik.mex.k8s.rollout.RolloutEvent}s and published on
     * the bus. The provisioning_records row is flipped to the
     * terminal status automatically — no more manual
     * markReady/markFailed from the caller.</p>
     *
     * <p>Returns the final {@link ApplyResult}; on a READY outcome
     * the row's status is READY, on FAILED / timeout it's FAILED.
     * Partial-apply failure before the watch even starts returns
     * the same result as {@link #apply}.</p>
     */
    public ApplyResult applyAndWatch(com.kubrik.mex.k8s.model.K8sClusterRef clusterRef,
                                      com.kubrik.mex.k8s.operator.OperatorAdapter adapter,
                                      com.kubrik.mex.k8s.provision.ProvisionModel model,
                                      com.kubrik.mex.k8s.rollout.RolloutWatcher watcher) throws java.io.IOException {
        ApplyResult applyResult = apply(clusterRef, adapter, model);
        if (!applyResult.ok()) return applyResult;

        long rowId = applyResult.provisioningId();
        com.kubrik.mex.k8s.rollout.RolloutWatcher.EventSink sink = e -> {
            try { eventDao.insert(e); }
            catch (java.sql.SQLException sqle) {
                log.debug("persist watch event {}: {}", rowId, sqle.toString());
            }
            // Re-map WARN/ERROR into ProvisionEvent.Progress so the UI
            // sees the transition without waiting for the terminal event.
            events.publishProvision(new ProvisionEvent.Progress(
                    rowId,
                    e.reason().orElse("Watch") + ": " + e.message().orElse(""),
                    e.at()));
        };

        com.kubrik.mex.k8s.rollout.RolloutWatcher.WatchResult wr =
                watcher.watch(clusterRef, adapter, model.namespace(),
                        model.deploymentName(), rowId, sink);

        try {
            if (wr.ok()) {
                markReady(rowId, model.deploymentName());
            } else {
                markFailed(rowId, wr.summary());
            }
        } catch (java.sql.SQLException sqle) {
            log.warn("mark terminal {} {}: {}", rowId, wr.status(), sqle.toString());
        }
        return applyResult;
    }

    public void cleanup(K8sClusterRef ref, ResourceCatalogue catalogue) throws IOException {
        ApiClient client;
        try { client = clientFactory.get(ref); }
        catch (IOException ioe) { throw ioe; }
        for (ResourceCatalogue.Ref r : catalogue.reversed()) {
            try { opener.delete(client, r); }
            catch (Exception e) {
                log.debug("cleanup delete {}/{} failed: {}",
                        r.kind(), r.name(), e.toString());
            }
        }
    }

    /* ============================ helpers ============================ */

    private void recordFail(long rowId, String reason) {
        try { markFailed(rowId, reason); }
        catch (java.sql.SQLException e) {
            log.warn("markFailed {}: {}", rowId, e.toString());
        }
    }

    private void recordProgress(long rowId, String step) {
        try {
            eventDao.insert(new RolloutEvent(rowId, System.currentTimeMillis(),
                    RolloutEvent.Source.APPLY, RolloutEvent.Severity.INFO,
                    java.util.Optional.of("AppliedStep"),
                    java.util.Optional.of(step),
                    java.util.Optional.empty()));
        } catch (java.sql.SQLException sqle) {
            log.debug("persist rollout event: {}", sqle.toString());
        }
        events.publishProvision(new ProvisionEvent.Progress(
                rowId, step, System.currentTimeMillis()));
    }

    static String manifestSha256(KubernetesManifests manifests) {
        StringBuilder sb = new StringBuilder();
        for (KubernetesManifests.Manifest m : manifests.documents()) {
            sb.append(m.kind()).append('/').append(m.name()).append('\n').append(m.yaml()).append("\n---\n");
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String apiVersionOf(String kind) {
        return switch (kind) {
            case "Secret", "ConfigMap" -> "v1";
            case "PodDisruptionBudget" -> "policy/v1";
            case "ServiceMonitor" -> "monitoring.coreos.com/v1";
            case "CronJob" -> "batch/v1";
            case "MongoDBCommunity" -> "mongodbcommunity.mongodb.com/v1";
            case "PerconaServerMongoDB" -> "psmdb.percona.com/v1";
            default -> "v1";
        };
    }

    /** Shape returned to the caller — carries the catalogue for cleanup + the hash for audit. */
    public record ApplyResult(long provisioningId, ResourceCatalogue catalogue,
                                String sha256, java.util.Optional<String> error) {
        public ApplyResult(long id, ResourceCatalogue c, String sha) {
            this(id, c, sha, java.util.Optional.empty());
        }
        public ApplyResult(long id, ResourceCatalogue c, String sha, String err) {
            this(id, c, sha, java.util.Optional.ofNullable(err));
        }
        public boolean ok() { return error.isEmpty(); }
    }

    /**
     * Seam for the YAML → server dispatch. Tests swap in a recorder;
     * the production {@link DefaultApplyOpener} uses the Kubernetes
     * Java client to server-side-apply every document.
     */
    public interface ApplyOpener {
        void apply(ApiClient client, ResourceCatalogue.Ref ref, String yaml) throws Exception;
        void delete(ApiClient client, ResourceCatalogue.Ref ref) throws Exception;
    }

}
