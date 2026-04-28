package com.kubrik.mex.k8s.operator.psmdb;

import com.kubrik.mex.k8s.operator.Capability;
import com.kubrik.mex.k8s.operator.DeploymentStatus;
import com.kubrik.mex.k8s.operator.KubernetesManifests;
import com.kubrik.mex.k8s.operator.OperatorAdapter;
import com.kubrik.mex.k8s.provision.OperatorId;
import com.kubrik.mex.k8s.provision.ProvisionModel;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * v2.8.1 Q2.8.1-F2 — Percona Server for MongoDB Operator adapter.
 *
 * <p>Richer capability set than MCO: PSMDB ships SHARDED,
 * NATIVE_BACKUP (PBM), and NATIVE_SERVICE_MONITOR (PMM) support.
 * The wizard's Topology step reads this to unlock the SHARDED
 * radio when the user selects PSMDB on the Prod profile.</p>
 */
public final class PsmdbAdapter implements OperatorAdapter {

    private static final Set<Capability> CAPS = EnumSet.of(
            Capability.SHARDED,
            Capability.NATIVE_BACKUP,
            Capability.OPERATOR_GENERATED_TLS,
            Capability.CERT_MANAGER_TLS,
            Capability.IN_PLACE_UPGRADE,
            Capability.NATIVE_SERVICE_MONITOR,
            Capability.ARBITER_MEMBERS);

    private final PsmdbCRRenderer renderer;
    private final PsmdbStatusParser parser;

    public PsmdbAdapter() {
        this(new PsmdbCRRenderer(), new PsmdbStatusParser());
    }

    public PsmdbAdapter(PsmdbCRRenderer renderer, PsmdbStatusParser parser) {
        this.renderer = renderer;
        this.parser = parser;
    }

    @Override public OperatorId id() { return OperatorId.PSMDB; }

    @Override public Set<Capability> capabilities() { return CAPS; }

    @Override public String crdGroup() { return "psmdb.percona.com"; }

    @Override public String crdKind() { return PsmdbCRRenderer.CRD_KIND; }

    @Override
    public KubernetesManifests render(ProvisionModel model) {
        return renderer.render(model);
    }

    @Override
    public DeploymentStatus parseStatus(Map<String, Object> crStatus,
                                          List<Map<String, Object>> pods,
                                          List<Map<String, Object>> events) {
        return parser.parse(crStatus, pods, events);
    }
}
