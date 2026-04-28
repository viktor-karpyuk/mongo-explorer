package com.kubrik.mex.k8s.compute.nodepool;

import com.kubrik.mex.k8s.compute.ComputeStrategy;
import com.kubrik.mex.k8s.compute.LabelPair;
import com.kubrik.mex.k8s.compute.SpreadScope;
import com.kubrik.mex.k8s.compute.Toleration;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.2 Q2.8.2-B — Asserts {@link NodePoolRenderer} mutates a pod
 * spec to carry exactly the three node-pool fields in the shape the
 * operator CRDs expect.
 */
class NodePoolRendererTest {

    @Test
    void none_strategy_leaves_pod_spec_untouched() {
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("containers", List.of());
        NodePoolRenderer.mutate(spec, ComputeStrategy.NONE, "prod-rs");
        assertEquals(1, spec.size(), "none strategy must not touch the spec");
        assertFalse(spec.containsKey("nodeSelector"));
        assertFalse(spec.containsKey("tolerations"));
        assertFalse(spec.containsKey("topologySpreadConstraints"));
    }

    @Test
    void node_pool_emits_selector_tolerations_and_scoped_spread() {
        Map<String, Object> spec = new LinkedHashMap<>();
        ComputeStrategy.NodePool np = new ComputeStrategy.NodePool(
                List.of(new LabelPair("workload", "mongodb"),
                        new LabelPair("tier", "prod")),
                List.of(new Toleration("dedicated", "mongo", Toleration.Effect.NO_SCHEDULE)),
                SpreadScope.WITHIN_POOL);

        NodePoolRenderer.mutate(spec, np, "prod-rs");

        @SuppressWarnings("unchecked")
        Map<String, Object> sel = (Map<String, Object>) spec.get("nodeSelector");
        assertEquals("mongodb", sel.get("workload"));
        assertEquals("prod", sel.get("tier"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> tol = (List<Map<String, Object>>) spec.get("tolerations");
        assertEquals(1, tol.size());
        assertEquals("dedicated", tol.get(0).get("key"));
        assertEquals("Equal", tol.get(0).get("operator"));
        assertEquals("NoSchedule", tol.get(0).get("effect"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> spread =
                (List<Map<String, Object>>) spec.get("topologySpreadConstraints");
        assertEquals(1, spread.size());
        assertEquals("kubernetes.io/hostname", spread.get(0).get("topologyKey"));
        assertEquals("DoNotSchedule", spread.get(0).get("whenUnsatisfiable"));
        @SuppressWarnings("unchecked")
        Map<String, Object> ls = (Map<String, Object>) spread.get(0).get("labelSelector");
        @SuppressWarnings("unchecked")
        Map<String, Object> ml = (Map<String, Object>) ls.get("matchLabels");
        assertEquals("prod-rs-svc", ml.get("app"));
    }

    @Test
    void toleration_with_null_value_renders_as_exists() {
        Map<String, Object> spec = new LinkedHashMap<>();
        ComputeStrategy.NodePool np = new ComputeStrategy.NodePool(
                List.of(new LabelPair("w", "v")),
                List.of(new Toleration("key", null, Toleration.Effect.NO_EXECUTE)),
                SpreadScope.WITHIN_POOL);
        NodePoolRenderer.mutate(spec, np, "deploy");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> tol = (List<Map<String, Object>>) spec.get("tolerations");
        assertEquals("Exists", tol.get(0).get("operator"));
        assertFalse(tol.get(0).containsKey("value"));
        assertEquals("NoExecute", tol.get(0).get("effect"));
    }

    @Test
    void existing_node_selector_is_merged_not_overwritten() {
        Map<String, Object> spec = new LinkedHashMap<>();
        Map<String, Object> existing = new LinkedHashMap<>();
        existing.put("kubernetes.io/os", "linux");
        spec.put("nodeSelector", existing);

        ComputeStrategy.NodePool np = new ComputeStrategy.NodePool(
                List.of(new LabelPair("workload", "mongodb")),
                List.of(), SpreadScope.WITHIN_POOL);
        NodePoolRenderer.mutate(spec, np, "deploy");

        @SuppressWarnings("unchecked")
        Map<String, Object> merged = (Map<String, Object>) spec.get("nodeSelector");
        assertEquals("linux", merged.get("kubernetes.io/os"));
        assertEquals("mongodb", merged.get("workload"));
    }

    @Test
    void existing_tolerations_are_appended_not_replaced() {
        Map<String, Object> spec = new LinkedHashMap<>();
        Map<String, Object> existingTol = new LinkedHashMap<>();
        existingTol.put("key", "gpu");
        existingTol.put("operator", "Exists");
        existingTol.put("effect", "NoSchedule");
        spec.put("tolerations", new java.util.ArrayList<>(List.of(existingTol)));

        ComputeStrategy.NodePool np = new ComputeStrategy.NodePool(
                List.of(new LabelPair("w", "v")),
                List.of(new Toleration("dedicated", "mongo", Toleration.Effect.NO_SCHEDULE)),
                SpreadScope.WITHIN_POOL);
        NodePoolRenderer.mutate(spec, np, "deploy");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> tol = (List<Map<String, Object>>) spec.get("tolerations");
        assertEquals(2, tol.size());
        assertEquals("gpu", tol.get(0).get("key"));
        assertEquals("dedicated", tol.get(1).get("key"));
    }
}
