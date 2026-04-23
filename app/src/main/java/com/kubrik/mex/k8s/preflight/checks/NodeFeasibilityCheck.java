package com.kubrik.mex.k8s.preflight.checks;

import com.kubrik.mex.k8s.preflight.PreflightCheck;
import com.kubrik.mex.k8s.preflight.PreflightResult;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeList;

import java.util.HashSet;
import java.util.Set;

/**
 * v2.8.1 Q2.8.1-G — Node count ≥ topology replica count, spread
 * feasibility across zones.
 *
 * <p>Single-node cluster + RS3 → FAIL (can't honour anti-affinity).
 * Three nodes in one zone + Prod spread locked on → WARN (the CR
 * will apply but topology spread may fight).</p>
 */
public final class NodeFeasibilityCheck implements PreflightCheck {

    public static final String ID = "preflight.node-feasibility";

    @Override public String id() { return ID; }
    @Override public String label() { return "Node feasibility"; }
    @Override public PreflightScope scope(ProvisionModel m) { return PreflightScope.ALWAYS; }

    @Override
    public PreflightResult run(ApiClient client, ProvisionModel m) {
        V1NodeList nodes;
        try {
            nodes = new CoreV1Api(client).listNode().execute();
        } catch (Exception e) {
            return PreflightResult.warn(ID,
                    "Node list failed: " + e.getMessage(),
                    "Caller may lack cluster-scope list nodes — pre-flight degrades to a warning.");
        }

        int nodeCount = nodes.getItems().size();
        int required = m.topology().replicasPerReplset();
        if (nodeCount < required) {
            return PreflightResult.fail(ID,
                    "Cluster has " + nodeCount + " node(s) but topology "
                    + m.topology() + " needs " + required + ".",
                    "Add nodes or downgrade topology (standalone needs 1, RS3 needs 3, RS5 needs 5).");
        }

        Set<String> zones = new HashSet<>();
        for (V1Node n : nodes.getItems()) {
            if (n.getMetadata() == null || n.getMetadata().getLabels() == null) continue;
            String zone = n.getMetadata().getLabels().get("topology.kubernetes.io/zone");
            if (zone != null) zones.add(zone);
        }
        if (m.scheduling().topologySpread() && zones.size() < 2) {
            return PreflightResult.warn(ID,
                    "Topology spread enabled but only " + zones.size() + " zone(s) visible.",
                    "Zone labels missing on some nodes, or the cluster is single-zone. "
                    + "Spread will degrade to best-effort.");
        }
        return PreflightResult.pass(ID);
    }
}
