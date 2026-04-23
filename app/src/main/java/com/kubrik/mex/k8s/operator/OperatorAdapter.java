package com.kubrik.mex.k8s.operator;

import com.kubrik.mex.k8s.provision.OperatorId;
import com.kubrik.mex.k8s.provision.ProvisionModel;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * v2.8.1 Q2.8.1-E1 — Per-operator plugin boundary.
 *
 * <p>Pure surface: no API calls, no clock reads, no randomness. A
 * deterministic {@link #render} lets {@link
 * com.kubrik.mex.cluster.safety.PreviewHashChecker} sign the
 * manifests into the typed-confirm dialog so a mid-preview tamper
 * attempt is caught at Apply.</p>
 *
 * <p>Two implementations ship in v2.8.1:
 * {@code com.kubrik.mex.k8s.operator.mco.McoAdapter} and
 * {@code com.kubrik.mex.k8s.operator.psmdb.PsmdbAdapter} (Q2.8.1-F).</p>
 */
public interface OperatorAdapter {

    /** Identity used by the wizard to key the adapter lookup. */
    OperatorId id();

    /** Feature flags that gate wizard steps (e.g. SHARDED hides for MCO). */
    Set<Capability> capabilities();

    /** CRD group this adapter targets (e.g. {@code mongodbcommunity.mongodb.com}). */
    String crdGroup();

    /** CRD kind this adapter targets (e.g. {@code MongoDBCommunity}). */
    String crdKind();

    /**
     * Render the deployment. Pure function of {@code model} — same
     * input always produces the same YAML bytes so the dry-run hash
     * is stable.
     */
    KubernetesManifests render(ProvisionModel model);

    /**
     * Project a live CR + pod + event snapshot into
     * {@link DeploymentStatus}. Inputs are plain maps to avoid
     * coupling the adapter to the generated model stubs; parsers
     * pluck the fields they need defensively.
     */
    DeploymentStatus parseStatus(Map<String, Object> crStatus,
                                   List<Map<String, Object>> pods,
                                   List<Map<String, Object>> events);
}
