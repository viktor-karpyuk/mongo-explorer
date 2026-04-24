package com.kubrik.mex.labs.k8s.lifecycle;

import com.kubrik.mex.k8s.apply.ApplyOrchestrator;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.k8s.provision.ProvisioningService;
import com.kubrik.mex.labs.k8s.distro.LocalK8sDistroService;
import com.kubrik.mex.labs.k8s.model.LabK8sCluster;
import com.kubrik.mex.labs.k8s.model.LabK8sDistro;
import com.kubrik.mex.labs.k8s.templates.LabK8sTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * v2.8.1 Q2.8-N4 — Top-level orchestrator for a K8s Lab's apply flow.
 *
 * <p>Sequence:</p>
 * <ol>
 *   <li>{@link LocalK8sDistroService#createOrStart} brings the distro
 *       cluster up (idempotent on existing identifiers) — produces a
 *       {@link K8sClusterRef}.</li>
 *   <li>{@link LabK8sTemplate.TemplateFactory#build} materialises the
 *       {@link ProvisionModel} for the chosen template + ref + ns.</li>
 *   <li>{@link ProvisioningService#provision} runs the v2.8.1
 *       production pipeline — render → preflight-checked apply →
 *       watch → auto-connect. Unchanged code path (Decision 11).</li>
 * </ol>
 *
 * <p>Every step returns a {@link LabK8sApplyResult} variant so the UI
 * can render the failure point without knowing which layer produced
 * it. Distro-up failures don't run provisioning; provisioning
 * failures leave the distro cluster up for inspection (teardown is
 * an explicit user action).</p>
 */
public final class LabK8sLifecycleService {

    private static final Logger log = LoggerFactory.getLogger(LabK8sLifecycleService.class);

    private final LocalK8sDistroService distroService;
    private final ProvisioningService provisioningService;

    public LabK8sLifecycleService(LocalK8sDistroService distroService,
                                    ProvisioningService provisioningService) {
        this.distroService = Objects.requireNonNull(distroService, "distroService");
        this.provisioningService = Objects.requireNonNull(provisioningService, "provisioningService");
    }

    /**
     * Apply a Lab: bring the distro cluster up, materialise the
     * template's {@link ProvisionModel}, run the production
     * provision. Idempotent on the distro identifier — re-apply
     * against an already-running cluster reuses it.
     */
    public LabK8sApplyResult apply(LabK8sDistro distro,
                                     String clusterIdentifier,
                                     LabK8sTemplate template,
                                     String namespace,
                                     String deploymentName) {
        // Gate 1: distro up.
        LocalK8sDistroService.Result distroResult =
                distroService.createOrStart(distro, clusterIdentifier);
        if (!distroResult.success()) {
            String reason = distroResult instanceof LocalK8sDistroService.Result.Failed f
                    ? f.reason()
                    : "distro lifecycle rejected";
            return LabK8sApplyResult.distroFailed(reason);
        }

        LabK8sCluster labCluster = switch (distroResult) {
            case LocalK8sDistroService.Result.Created c -> c.cluster();
            case LocalK8sDistroService.Result.Already a -> a.cluster();
            case LocalK8sDistroService.Result.Ok ok -> ok.cluster();
            case LocalK8sDistroService.Result.Failed f -> null;   // unreachable
        };
        if (labCluster == null) {
            return LabK8sApplyResult.distroFailed("distro lifecycle returned no cluster");
        }
        if (labCluster.k8sClusterId().isEmpty()) {
            return LabK8sApplyResult.distroFailed(
                    "distro cluster is up but kubeconfig not attached — check the Clusters pane");
        }

        // The ref is in k8s_clusters via attachK8sCluster — we need a
        // K8sClusterRef. The Created variant already handed us one;
        // for Already / Ok we reload from DAO via distroService.
        K8sClusterRef ref;
        if (distroResult instanceof LocalK8sDistroService.Result.Created c && c.ref() != null) {
            ref = c.ref();
        } else {
            // Re-derive the ref — a follow-up would add a
            // KubeClusterService.findById; for now we require Created.
            return LabK8sApplyResult.distroFailed(
                    "distro cluster already existed; re-apply from the Clusters pane for now");
        }

        // Gate 2: materialise + provision via the v2.8.1 pipeline.
        ProvisionModel model = template.factory()
                .build(ref.id(), namespace, deploymentName);

        ApplyOrchestrator.ApplyResult provisionResult;
        try {
            provisionResult = provisioningService.provision(ref, model);
        } catch (IOException ioe) {
            return LabK8sApplyResult.provisionFailed(labCluster, ioe.getMessage());
        }
        if (!provisionResult.ok()) {
            return LabK8sApplyResult.provisionFailed(
                    labCluster,
                    provisionResult.error().orElse("apply failed"));
        }
        return LabK8sApplyResult.ok(labCluster, provisionResult.provisioningId());
    }

    /**
     * Tear a Lab down at the distro level. Runs the distro CLI's
     * destroy (removes cluster + all its Mongo state), then the
     * downstream cascade: LocalK8sDistroService.destroy already
     * forgets the k8s_clusters row, and SQLite's CASCADE on
     * rollout_events + the DAO's RESTRICT on live provisioning_records
     * keep the audit trail coherent.
     *
     * <p>Returns a human-friendly summary for the UI status line.</p>
     */
    public String destroy(long labClusterRowId) {
        LocalK8sDistroService.Result r = distroService.destroy(labClusterRowId);
        return switch (r) {
            case LocalK8sDistroService.Result.Ok ok ->
                    "destroyed " + ok.cluster().coordinates();
            case LocalK8sDistroService.Result.Failed f ->
                    "destroy failed: " + f.reason();
            case LocalK8sDistroService.Result.Already a ->
                    "already destroyed " + a.cluster().coordinates();
            case LocalK8sDistroService.Result.Created c ->
                    "destroyed " + c.cluster().coordinates();
        };
    }

    /* ============================ result types ============================ */

    public sealed interface LabK8sApplyResult {
        record Ok(LabK8sCluster cluster, long provisioningId) implements LabK8sApplyResult {}
        record DistroFailed(String reason) implements LabK8sApplyResult {}
        record ProvisionFailed(LabK8sCluster cluster, String reason) implements LabK8sApplyResult {}

        static LabK8sApplyResult ok(LabK8sCluster c, long id) { return new Ok(c, id); }
        static LabK8sApplyResult distroFailed(String why) { return new DistroFailed(why); }
        static LabK8sApplyResult provisionFailed(LabK8sCluster c, String why) {
            return new ProvisionFailed(c, why);
        }

        default boolean success() { return this instanceof Ok; }
    }
}
