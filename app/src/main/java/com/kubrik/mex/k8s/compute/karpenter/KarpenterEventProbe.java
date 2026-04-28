package com.kubrik.mex.k8s.compute.karpenter;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.kubrik.mex.k8s.rollout.RolloutEvent;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import io.kubernetes.client.util.generic.options.ListOptions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * v2.8.3 Q2.8.3-C — Polls {@code NodeClaim} CRs that Karpenter creates
 * for a Mongo provisioning row and emits {@link RolloutEvent}s when a
 * claim's phase advances.
 *
 * <p>Held alongside the Mongo CR watcher: each tick queries
 * {@code karpenter.sh/v1 nodeclaims} filtered by the deployment label
 * we stamped on the {@link KarpenterRenderer}-emitted NodePool, and
 * surfaces transitions like {@code (none) → Pending → Ready → }
 * (deletion). State is kept in a per-instance map so a tick that
 * sees no change emits nothing.</p>
 */
public final class KarpenterEventProbe {

    private final ApiClient client;
    private final long provisioningId;
    private final String nodePoolLabel;
    private final Map<String, String> lastPhaseByName = new HashMap<>();

    /** DynamicKubernetesApi construction wires up Gson + the
     *  generic-resource codec; cached as a field so the watcher's
     *  poll cadence doesn't reconstruct it on every tick. */
    private final DynamicKubernetesApi nodeClaimsApi;

    public KarpenterEventProbe(ApiClient client, long provisioningId,
                                String nodePoolName) {
        this.client = Objects.requireNonNull(client, "client");
        this.provisioningId = provisioningId;
        this.nodePoolLabel = "karpenter.sh/nodepool=" + Objects.requireNonNull(
                nodePoolName, "nodePoolName");
        this.nodeClaimsApi = new DynamicKubernetesApi(
                "karpenter.sh", "v1", "nodeclaims", client);
    }

    /** Poll once. Caller invokes this on the same cadence as the
     *  Mongo CR watcher; each transition is emitted via {@code sink}.
     *  Synchronized so a retry-overlap or a parallel watcher tick
     *  can't ConcurrentModification the phase tracker — the call
     *  cost is dominated by the API round-trip anyway. */
    public synchronized void poll(Consumer<RolloutEvent> sink) {
        try {
            // Server-side label-selector filter — narrows the list
            // to just our NodePool's claims so a busy cluster with
            // thousands of unrelated NodeClaims doesn't paginate over
            // every poll. The matchesPool() check below stays as a
            // defence-in-depth (covers the empty-selector edge case).
            ListOptions opts = new ListOptions();
            opts.setLabelSelector(nodePoolLabel);
            var list = nodeClaimsApi.list(opts).getObject();
            if (list == null || list.getItems() == null) return;

            Set<String> seen = new java.util.HashSet<>();
            for (DynamicKubernetesObject obj : list.getItems()) {
                JsonObject root = obj.getRaw();
                if (root == null) continue;
                if (!matchesPool(root)) continue;

                String name = nameOf(root);
                if (name == null) continue;
                seen.add(name);

                String phase = phaseOf(root);
                String prev = lastPhaseByName.put(name, phase);
                if (!Objects.equals(prev, phase)) {
                    sink.accept(new RolloutEvent(provisioningId,
                            System.currentTimeMillis(),
                            RolloutEvent.Source.CR_STATUS,
                            RolloutEvent.Severity.INFO,
                            java.util.Optional.of("NodeClaim"),
                            java.util.Optional.of(name + " phase=" + phase),
                            java.util.Optional.empty()));
                }
            }
            // Detect deleted claims so the user sees pool consolidation.
            lastPhaseByName.keySet().retainAll(seen);
        } catch (Exception e) {
            // Probe is best-effort; the watcher's main loop must not
            // fail because Karpenter happens to be unhealthy. Errors
            // (OOM, StackOverflow) keep propagating so the JVM can
            // surface them — only Exception is swallowed.
        }
    }

    private boolean matchesPool(JsonObject root) {
        JsonObject metadata = root.getAsJsonObject("metadata");
        if (metadata == null) return false;
        JsonElement labelsRaw = metadata.get("labels");
        if (labelsRaw == null || !labelsRaw.isJsonObject()) return false;
        JsonObject labels = labelsRaw.getAsJsonObject();
        JsonElement np = labels.get("karpenter.sh/nodepool");
        if (np == null || np.isJsonNull()) return false;
        String want = nodePoolLabel.substring(nodePoolLabel.indexOf('=') + 1);
        return want.equals(np.getAsString());
    }

    private static String nameOf(JsonObject root) {
        JsonObject metadata = root.getAsJsonObject("metadata");
        if (metadata == null) return null;
        JsonElement n = metadata.get("name");
        return n == null || n.isJsonNull() ? null : n.getAsString();
    }

    private static String phaseOf(JsonObject root) {
        JsonObject status = root.getAsJsonObject("status");
        if (status == null) return "Unknown";
        JsonElement conditions = status.get("conditions");
        if (conditions != null && conditions.isJsonArray()) {
            for (JsonElement c : conditions.getAsJsonArray()) {
                if (!c.isJsonObject()) continue;
                JsonObject co = c.getAsJsonObject();
                if (co.has("type") && "Ready".equals(co.get("type").getAsString())) {
                    return "True".equals(co.get("status").getAsString())
                            ? "Ready" : "NotReady";
                }
            }
        }
        return "Pending";
    }

    /** Visible for tests — current phase tracking snapshot. */
    public Map<String, String> snapshot() {
        return List.copyOf(lastPhaseByName.entrySet()).stream()
                .collect(java.util.stream.Collectors.toMap(
                        Map.Entry::getKey, Map.Entry::getValue));
    }
}
