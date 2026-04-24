package com.kubrik.mex.k8s.compute;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * v2.8.2 Q2.8.2-A — Canonical JSON codec for {@link ComputeStrategy}.
 *
 * <p>Serialisation target: {@code provisioning_records.compute_strategy_json}
 * (nullable; NULL means "v2.8.0 default scheduler"). The wire format
 * is intentionally explicit and human-readable so audit diff tools
 * and post-mortems can inspect it without code:</p>
 *
 * <pre>{@code
 *   { "type": "node_pool",
 *     "selector": { "workload": "mongodb", "tier": "prod" },
 *     "tolerations": [
 *       { "key": "dedicated", "value": "mongo", "effect": "NoSchedule" }
 *     ],
 *     "spread": "within_pool" }
 * }</pre>
 *
 * <p>Unknown {@code type} values round-trip as {@link
 * ComputeStrategy.None} with a warning logged by the DAO — a forward-
 * compatibility hatch for rows written by later Mongo Explorer
 * versions.</p>
 */
public final class ComputeStrategyJson {

    private static final ObjectMapper M = new ObjectMapper();

    private ComputeStrategyJson() {}

    public static String toJson(ComputeStrategy s) {
        if (s == null || s instanceof ComputeStrategy.None) return null;
        ObjectNode root = M.createObjectNode();
        switch (s) {
            case ComputeStrategy.NodePool np -> {
                root.put("type", "node_pool");
                ObjectNode sel = root.putObject("selector");
                for (LabelPair p : np.selector()) sel.put(p.key(), p.value());
                ArrayNode tol = root.putArray("tolerations");
                for (Toleration t : np.tolerations()) {
                    ObjectNode o = tol.addObject();
                    o.put("key", t.key());
                    if (t.value() != null) o.put("value", t.value());
                    o.put("effect", t.effectYamlValue());
                }
                root.put("spread", np.spreadScope().name().toLowerCase());
            }
            case ComputeStrategy.Karpenter k -> root.put("type", "karpenter");
            case ComputeStrategy.ManagedPool mp -> root.put("type", "managed_pool");
            case ComputeStrategy.None n -> { /* filtered earlier */ }
        }
        try { return M.writeValueAsString(root); }
        catch (Exception e) { throw new IllegalStateException(
                "serialise compute strategy: " + e.getMessage(), e); }
    }

    public static ComputeStrategy fromJson(String json) {
        if (json == null || json.isBlank()) return ComputeStrategy.NONE;
        JsonNode root;
        try { root = M.readTree(json); }
        catch (IOException ioe) { throw new IllegalArgumentException(
                "parse compute_strategy_json: " + ioe.getMessage(), ioe); }
        String type = root.path("type").asText("none");
        return switch (type) {
            case "node_pool" -> parseNodePool(root);
            case "karpenter" -> new ComputeStrategy.Karpenter();
            case "managed_pool" -> new ComputeStrategy.ManagedPool();
            default -> ComputeStrategy.NONE;
        };
    }

    private static ComputeStrategy.NodePool parseNodePool(JsonNode root) {
        List<LabelPair> selector = new ArrayList<>();
        JsonNode sel = root.path("selector");
        if (sel.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> it = sel.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                selector.add(new LabelPair(e.getKey(), e.getValue().asText()));
            }
        }
        List<Toleration> tolerations = new ArrayList<>();
        JsonNode tol = root.path("tolerations");
        if (tol.isArray()) {
            for (JsonNode t : tol) {
                String key = t.path("key").asText(null);
                if (key == null) continue;
                String value = t.path("value").isMissingNode() ? null : t.path("value").asText();
                String effectRaw = t.path("effect").asText("NoSchedule");
                Toleration.Effect effect = "NoExecute".equals(effectRaw)
                        ? Toleration.Effect.NO_EXECUTE
                        : Toleration.Effect.NO_SCHEDULE;
                tolerations.add(new Toleration(key, value, effect));
            }
        }
        SpreadScope scope = SpreadScope.WITHIN_POOL; // only value shipped in v2.8.2
        return new ComputeStrategy.NodePool(selector, tolerations, scope);
    }
}
