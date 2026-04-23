package com.kubrik.mex.k8s.operator.mco;

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
 * v2.8.1 Q2.8.1-E3 — MongoDB Community Operator adapter.
 *
 * <p>Thin façade: every heavy lift is delegated to
 * {@link McoCRRenderer} (pure YAML build) and
 * {@link McoStatusParser} (pure map parse). Capabilities table
 * captures the operator's blessed-version feature matrix so the
 * wizard can hide irrelevant steps.</p>
 */
public final class McoAdapter implements OperatorAdapter {

    private static final Set<Capability> CAPS = EnumSet.of(
            Capability.OPERATOR_GENERATED_TLS,
            Capability.CERT_MANAGER_TLS,
            Capability.IN_PLACE_UPGRADE,
            Capability.ARBITER_MEMBERS
            // NOT supported: SHARDED, NATIVE_BACKUP, NATIVE_SERVICE_MONITOR
    );

    private final McoCRRenderer renderer;
    private final McoStatusParser parser;

    public McoAdapter() {
        this(new McoCRRenderer(), new McoStatusParser());
    }

    public McoAdapter(McoCRRenderer renderer, McoStatusParser parser) {
        this.renderer = renderer;
        this.parser = parser;
    }

    @Override public OperatorId id() { return OperatorId.MCO; }

    @Override public Set<Capability> capabilities() { return CAPS; }

    @Override public String crdGroup() { return "mongodbcommunity.mongodb.com"; }

    @Override public String crdKind() { return McoCRRenderer.CRD_KIND; }

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
