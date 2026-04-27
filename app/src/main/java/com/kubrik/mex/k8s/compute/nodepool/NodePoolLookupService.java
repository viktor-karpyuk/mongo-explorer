package com.kubrik.mex.k8s.compute.nodepool;

import com.kubrik.mex.k8s.compute.LabelPair;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeCondition;
import io.kubernetes.client.openapi.models.V1NodeList;
import io.kubernetes.client.openapi.models.V1Taint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * v2.8.2 Q2.8.2-C — Thin wrapper around {@code /api/v1/nodes} that
 * answers the four questions the node-pool preflight checks need:
 *
 * <ol>
 *   <li>Which nodes match the pool's label selector? (PRE-NP-1)</li>
 *   <li>Which taints does every matched node carry? (PRE-NP-2)</li>
 *   <li>How many of those matched nodes are Ready? (PRE-NP-3)</li>
 *   <li>What zones (topology.kubernetes.io/zone) are represented? (PRE-NP-4)</li>
 * </ol>
 *
 * <p>Kept out of the preflight package so the UI's "Pool lookup"
 * widget can reuse the same query without bouncing through a check.</p>
 */
public final class NodePoolLookupService {

    public record PoolSnapshot(
            List<String> matchedNodeNames,
            int readyCount,
            List<V1Taint> commonTaints,
            Set<String> zones
    ) {
        public boolean empty() { return matchedNodeNames.isEmpty(); }
    }

    private final ApiClient client;

    public NodePoolLookupService(ApiClient client) {
        this.client = client;
    }

    /**
     * Query every node and filter by the selector (AND semantics).
     * The blank-selector case returns every node — it's the wizard's
     * job to enforce at least one label before calling us.
     */
    public PoolSnapshot snapshot(List<LabelPair> selector) throws ApiException {
        // Push the selector to the server side via a labelSelector
        // expression — a busy cluster (>500 nodes) shouldn't paginate
        // every node into the JVM just to filter to a few. The
        // client-side matches() check below stays as defence-in-depth
        // (covers the empty-selector + edge cases the server may
        // not).
        String labelSelector = renderLabelSelector(selector);
        CoreV1Api api = new CoreV1Api(client);
        List<String> matchedNames = new ArrayList<>();
        int ready = 0;
        Set<String> zones = new java.util.LinkedHashSet<>();
        List<V1Taint> commonTaints = null;

        // Manual pagination — listNode().continueX().execute() loops
        // until the server returns no continue token. Single page
        // (continue == null) is the common case for clusters under a
        // few hundred nodes.
        String continueToken = null;
        do {
            CoreV1Api.APIlistNodeRequest req = api.listNode();
            if (labelSelector != null) req = req.labelSelector(labelSelector);
            if (continueToken != null) req = req._continue(continueToken);
            req = req.limit(500);
            V1NodeList list = req.execute();
            for (V1Node n : list.getItems()) {
                Map<String, String> labels = n.getMetadata() == null
                        ? Map.of() : n.getMetadata().getLabels() == null
                        ? Map.of() : n.getMetadata().getLabels();
                if (!matches(labels, selector)) continue;
                matchedNames.add(n.getMetadata().getName());
                if (isReady(n)) ready++;
                String zone = labels.get("topology.kubernetes.io/zone");
                if (zone != null) zones.add(zone);

                List<V1Taint> taints = n.getSpec() == null || n.getSpec().getTaints() == null
                        ? Collections.emptyList()
                        : n.getSpec().getTaints();
                if (commonTaints == null) {
                    commonTaints = new ArrayList<>(taints);
                } else {
                    commonTaints.retainAll(taints);
                }
            }
            continueToken = list.getMetadata() == null ? null
                    : list.getMetadata().getContinue();
        } while (continueToken != null && !continueToken.isEmpty());

        return new PoolSnapshot(
                List.copyOf(matchedNames),
                ready,
                commonTaints == null ? List.of() : List.copyOf(commonTaints),
                Set.copyOf(zones));
    }

    /** Render an AND-of-equalities label selector for the K8s API.
     *  Returns null for an empty selector (matches every node). */
    private static String renderLabelSelector(List<LabelPair> selector) {
        if (selector == null || selector.isEmpty()) return null;
        StringBuilder sb = new StringBuilder();
        for (LabelPair p : selector) {
            if (sb.length() > 0) sb.append(',');
            sb.append(p.key()).append('=').append(p.value());
        }
        return sb.toString();
    }

    /** Visible for tests — AND-match node labels against a selector. */
    static boolean matches(Map<String, String> labels, List<LabelPair> selector) {
        for (LabelPair p : selector) {
            if (!p.value().equals(labels.get(p.key()))) return false;
        }
        return true;
    }

    private static boolean isReady(V1Node n) {
        if (n.getStatus() == null || n.getStatus().getConditions() == null) return false;
        for (V1NodeCondition c : n.getStatus().getConditions()) {
            if ("Ready".equals(c.getType())) return "True".equals(c.getStatus());
        }
        return false;
    }
}
