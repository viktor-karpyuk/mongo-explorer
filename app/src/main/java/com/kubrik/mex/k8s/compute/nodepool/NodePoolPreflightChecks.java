package com.kubrik.mex.k8s.compute.nodepool;

import com.kubrik.mex.k8s.compute.ComputeStrategy;
import com.kubrik.mex.k8s.compute.Toleration;
import com.kubrik.mex.k8s.preflight.PreflightCheck;
import com.kubrik.mex.k8s.preflight.PreflightResult;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.models.V1Taint;

import java.util.ArrayList;
import java.util.List;

/**
 * v2.8.2 Q2.8.2-C — The four {@code PRE-NP-*} checks applied to a
 * node-pool targeting strategy. Instantiated as one
 * {@link PreflightCheck} per rule so each row in the preflight
 * viewer has its own pass/fail chip.
 *
 * <p>Decision 1 (milestone §7): {@code PRE-NP-1..3} are hard-FAIL —
 * a Mongo provision aimed at a missing / mismatched / under-capacity
 * pool has no "warn and proceed" value. {@code PRE-NP-4} (spread
 * feasibility) is WARN so single-zone Dev/Test pools still work.</p>
 */
public final class NodePoolPreflightChecks {

    private NodePoolPreflightChecks() {}

    public static List<PreflightCheck> all() {
        return List.of(
                new LabelsCheck(),
                new TaintsCheck(),
                new CapacityCheck(),
                new SpreadCheck());
    }

    /* ============================ PRE-NP-1 ============================ */

    public static final class LabelsCheck implements PreflightCheck {
        public static final String ID = "preflight.node-pool.labels";

        @Override public String id() { return ID; }
        @Override public String label() { return "Node-pool labels resolve"; }
        @Override public PreflightScope scope(ProvisionModel m) {
            return m.computeStrategy() instanceof ComputeStrategy.NodePool
                    ? PreflightScope.CONDITIONAL : PreflightScope.SKIP;
        }

        @Override
        public PreflightResult run(ApiClient client, ProvisionModel m) {
            ComputeStrategy.NodePool np = (ComputeStrategy.NodePool) m.computeStrategy();
            try {
                NodePoolLookupService.PoolSnapshot snap =
                        new NodePoolLookupService(client).snapshot(np.selector());
                if (snap.empty()) {
                    return PreflightResult.fail(ID,
                            "No nodes match the pool selector "
                            + renderSelector(np),
                            "Have your platform team label at least one node with "
                            + renderSelector(np) + " before retrying.");
                }
                return PreflightResult.pass(ID);
            } catch (Exception e) {
                return PreflightResult.fail(ID,
                        "Node listing failed: " + e.getMessage(),
                        "Verify the ServiceAccount has `nodes` list permission.");
            }
        }
    }

    /* ============================ PRE-NP-2 ============================ */

    public static final class TaintsCheck implements PreflightCheck {
        public static final String ID = "preflight.node-pool.taints";

        @Override public String id() { return ID; }
        @Override public String label() { return "Node-pool taints tolerated"; }
        @Override public PreflightScope scope(ProvisionModel m) {
            return m.computeStrategy() instanceof ComputeStrategy.NodePool
                    ? PreflightScope.CONDITIONAL : PreflightScope.SKIP;
        }

        @Override
        public PreflightResult run(ApiClient client, ProvisionModel m) {
            ComputeStrategy.NodePool np = (ComputeStrategy.NodePool) m.computeStrategy();
            try {
                NodePoolLookupService.PoolSnapshot snap =
                        new NodePoolLookupService(client).snapshot(np.selector());
                if (snap.empty()) {
                    // LabelsCheck will already have failed; SKIP would
                    // hide a row the user needs to see. Pass here to
                    // avoid double-reporting.
                    return PreflightResult.pass(ID);
                }
                List<V1Taint> untolerated = new ArrayList<>();
                for (V1Taint t : snap.commonTaints()) {
                    if (!isTolerated(t, np.tolerations())) untolerated.add(t);
                }
                if (untolerated.isEmpty()) return PreflightResult.pass(ID);
                return PreflightResult.fail(ID,
                        "Pool carries taints that the rendered Mongo pod does not tolerate: "
                        + renderTaints(untolerated),
                        "Add matching tolerations in the Dedicated-compute step, "
                        + "or remove the taints from the pool nodes.");
            } catch (Exception e) {
                return PreflightResult.fail(ID,
                        "Taint probe failed: " + e.getMessage(),
                        "Retry once the API server is reachable.");
            }
        }

        /** Visible for tests. */
        static boolean isTolerated(V1Taint t, List<Toleration> tolerations) {
            for (Toleration tol : tolerations) {
                if (!tol.key().equals(t.getKey())) continue;
                if (!effectMatches(tol, t.getEffect())) continue;
                if (tol.value() == null || tol.value().equals(t.getValue())) return true;
            }
            return false;
        }

        private static boolean effectMatches(Toleration tol, String effect) {
            if (effect == null) return true; // "match any" is the K8s default
            return tol.effectYamlValue().equals(effect);
        }
    }

    /* ============================ PRE-NP-3 ============================ */

    public static final class CapacityCheck implements PreflightCheck {
        public static final String ID = "preflight.node-pool.capacity";

        @Override public String id() { return ID; }
        @Override public String label() { return "Node-pool has capacity"; }
        @Override public PreflightScope scope(ProvisionModel m) {
            return m.computeStrategy() instanceof ComputeStrategy.NodePool
                    ? PreflightScope.CONDITIONAL : PreflightScope.SKIP;
        }

        @Override
        public PreflightResult run(ApiClient client, ProvisionModel m) {
            ComputeStrategy.NodePool np = (ComputeStrategy.NodePool) m.computeStrategy();
            int needed = m.topology().replicasPerReplset();
            try {
                NodePoolLookupService.PoolSnapshot snap =
                        new NodePoolLookupService(client).snapshot(np.selector());
                if (snap.empty()) return PreflightResult.pass(ID); // deferred to LabelsCheck
                if (snap.readyCount() < needed) {
                    return PreflightResult.fail(ID,
                            "Pool has " + snap.readyCount() + " Ready node(s); topology needs "
                            + needed + ".",
                            "Scale the pool or pick a smaller topology.");
                }
                return PreflightResult.pass(ID);
            } catch (Exception e) {
                return PreflightResult.fail(ID,
                        "Capacity probe failed: " + e.getMessage(),
                        "Retry once the API server is reachable.");
            }
        }
    }

    /* ============================ PRE-NP-4 ============================ */

    public static final class SpreadCheck implements PreflightCheck {
        public static final String ID = "preflight.node-pool.spread";

        @Override public String id() { return ID; }
        @Override public String label() { return "Node-pool zone spread"; }
        @Override public PreflightScope scope(ProvisionModel m) {
            return m.computeStrategy() instanceof ComputeStrategy.NodePool
                    ? PreflightScope.CONDITIONAL : PreflightScope.SKIP;
        }

        @Override
        public PreflightResult run(ApiClient client, ProvisionModel m) {
            ComputeStrategy.NodePool np = (ComputeStrategy.NodePool) m.computeStrategy();
            int needed = m.topology().replicasPerReplset();
            try {
                NodePoolLookupService.PoolSnapshot snap =
                        new NodePoolLookupService(client).snapshot(np.selector());
                if (snap.empty()) return PreflightResult.pass(ID);
                int zoneCount = snap.zones().size();
                if (zoneCount >= 2) return PreflightResult.pass(ID);
                return PreflightResult.warn(ID,
                        "Pool spans " + (zoneCount == 0 ? "no known zones" : zoneCount + " zone(s)")
                        + " — a " + needed + "-node topology can't survive a zone outage.",
                        "Add nodes from additional failure zones, or acknowledge this for Dev/Test.");
            } catch (Exception e) {
                return PreflightResult.warn(ID,
                        "Spread probe failed: " + e.getMessage(),
                        "Retry once the API server is reachable.");
            }
        }
    }

    /* ============================ helpers ============================ */

    private static String renderSelector(ComputeStrategy.NodePool np) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (var p : np.selector()) {
            if (!first) sb.append(", ");
            sb.append(p.key()).append('=').append(p.value());
            first = false;
        }
        return sb.append('}').toString();
    }

    private static String renderTaints(List<V1Taint> taints) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < taints.size(); i++) {
            if (i > 0) sb.append(", ");
            V1Taint t = taints.get(i);
            sb.append(t.getKey());
            if (t.getValue() != null) sb.append('=').append(t.getValue());
            sb.append(':').append(t.getEffect() == null ? "NoSchedule" : t.getEffect());
        }
        return sb.toString();
    }
}
