package com.kubrik.mex.k8s.compute.nodepool;

import com.kubrik.mex.k8s.compute.ComputeStrategy;
import com.kubrik.mex.k8s.compute.LabelPair;
import com.kubrik.mex.k8s.compute.SpreadScope;
import com.kubrik.mex.k8s.compute.Toleration;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * v2.8.2 Q2.8.2-B — Injects node-pool targeting into an operator's
 * rendered pod spec.
 *
 * <p>Given the pod-spec map an operator renderer has already built,
 * this class merges in:</p>
 * <ul>
 *   <li>{@code nodeSelector} — from the strategy's {@link LabelPair}s.</li>
 *   <li>{@code tolerations} — one entry per {@link Toleration} in the
 *       strategy, appended to anything the renderer already set.</li>
 *   <li>{@code topologySpreadConstraints} — a pool-scoped spread
 *       replacing whatever the renderer put there, with a
 *       {@code labelSelector} narrowed to this deployment only
 *       (Decision 4, milestone-v2.8.2.md).</li>
 * </ul>
 *
 * <p>Pure + deterministic so the preview-hash invariant holds
 * (production callers must call this with a {@link LinkedHashMap}
 * spec to keep YAML byte-stable).</p>
 */
public final class NodePoolRenderer {

    private NodePoolRenderer() {}

    /**
     * Mutate {@code podSpec} in-place to layer a {@link
     * ComputeStrategy.NodePool} strategy onto it. No-op when
     * {@code strategy} is anything else (callers don't need to
     * branch at the call site).
     *
     * @param podSpec  the operator's pod spec map (ordered insertion).
     * @param strategy the compute strategy the user picked.
     * @param podLabelSelector the label selector value (typically the
     *                  deployment name) used to narrow topologySpread
     *                  to this deployment's pods only.
     */
    public static void mutate(Map<String, Object> podSpec,
                               ComputeStrategy strategy,
                               String podLabelSelector) {
        if (!(strategy instanceof ComputeStrategy.NodePool np)) return;

        // nodeSelector — merge into whatever the renderer already set.
        @SuppressWarnings("unchecked")
        Map<String, Object> existingSelector =
                (Map<String, Object>) podSpec.get("nodeSelector");
        Map<String, Object> selector = existingSelector != null
                ? new LinkedHashMap<>(existingSelector)
                : new LinkedHashMap<>();
        for (LabelPair p : np.selector()) selector.put(p.key(), p.value());
        podSpec.put("nodeSelector", selector);

        // tolerations — append our entries to whatever exists.
        List<Map<String, Object>> existingTolerations = coerceToleration(podSpec.get("tolerations"));
        List<Map<String, Object>> merged = new ArrayList<>(existingTolerations);
        for (Toleration t : np.tolerations()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("key", t.key());
            if (t.value() != null) {
                entry.put("operator", "Equal");
                entry.put("value", t.value());
            } else {
                entry.put("operator", "Exists");
            }
            entry.put("effect", t.effectYamlValue());
            merged.add(entry);
        }
        if (!merged.isEmpty()) podSpec.put("tolerations", merged);

        // topologySpreadConstraints — APPEND the pool-scoped variant
        // alongside any existing constraints (e.g. the operator's
        // zone-spread). Replacing them stomped multi-zone resilience
        // when both spreads are valid + complementary. Decision 4
        // narrows the labelSelector to *this deployment* via the
        // `app` matchLabel so the pool-scoped entry doesn't compete
        // with namespace-wide siblings.
        if (np.spreadScope() == SpreadScope.WITHIN_POOL) {
            Map<String, Object> tsc = new LinkedHashMap<>();
            tsc.put("maxSkew", 1);
            tsc.put("topologyKey", "kubernetes.io/hostname");
            tsc.put("whenUnsatisfiable", "DoNotSchedule");
            tsc.put("labelSelector", Map.of("matchLabels",
                    Map.of("app", podLabelSelector + "-svc")));
            List<Map<String, Object>> existingSpread = coerceSpread(
                    podSpec.get("topologySpreadConstraints"));
            existingSpread.add(tsc);
            podSpec.put("topologySpreadConstraints", existingSpread);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> coerceSpread(Object raw) {
        if (raw instanceof List<?> list) {
            List<Map<String, Object>> out = new ArrayList<>(list.size() + 1);
            for (Object o : list) {
                if (o instanceof Map<?, ?> m) {
                    out.add(new LinkedHashMap<>((Map<String, Object>) m));
                }
            }
            return out;
        }
        return new ArrayList<>();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> coerceToleration(Object raw) {
        if (raw instanceof List<?> list) {
            List<Map<String, Object>> out = new ArrayList<>(list.size());
            for (Object o : list) {
                if (o instanceof Map<?, ?> m) {
                    out.add(new LinkedHashMap<>((Map<String, Object>) m));
                }
            }
            return out;
        }
        return new ArrayList<>();
    }
}
