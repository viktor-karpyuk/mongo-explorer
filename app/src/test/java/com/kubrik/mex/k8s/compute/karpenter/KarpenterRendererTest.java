package com.kubrik.mex.k8s.compute.karpenter;

import com.kubrik.mex.k8s.compute.ComputeStrategy;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.3 Q2.8.3-E — Deterministic golden-ish asserts for the
 * Karpenter NodePool + pod mutations.
 */
class KarpenterRendererTest {

    @Test
    void karpenter_render_emits_nodepool_and_mutates_pod() {
        ComputeStrategy.Karpenter k = new ComputeStrategy.Karpenter(
                KarpenterSpec.sensibleAwsDefaults("prod-rs"));
        Map<String, Object> pod = new LinkedHashMap<>();
        String yaml = new KarpenterRenderer().render(k, "prod-rs", pod);

        // NodePool YAML spot checks. Jackson YAML quotes string
        // values, so assert against the quoted form.
        assertTrue(yaml.contains("karpenter.sh/v1"), yaml);
        assertTrue(yaml.contains("NodePool"));
        assertTrue(yaml.contains("mex-prod-rs"));
        assertTrue(yaml.contains("karpenter.sh/capacity-type"));
        assertTrue(yaml.contains("karpenter.k8s.aws/instance-family"));
        assertTrue(yaml.contains("kubernetes.io/arch"));
        assertTrue(yaml.contains("mex.deployment"));
        assertTrue(yaml.contains("WhenUnderutilized"));
        assertTrue(yaml.contains("expireAfter"));
        assertTrue(yaml.contains("limits"));

        // Pod mutations: the bridge NodePool should have added
        // nodeSelector + tolerations + topologySpread.
        @SuppressWarnings("unchecked")
        Map<String, Object> sel = (Map<String, Object>) pod.get("nodeSelector");
        assertEquals("mex-prod-rs", sel.get("karpenter.sh/nodepool"));

        @SuppressWarnings("unchecked")
        java.util.List<Map<String, Object>> tol =
                (java.util.List<Map<String, Object>>) pod.get("tolerations");
        assertEquals(1, tol.size());
        assertEquals("mex.deployment", tol.get(0).get("key"));
        assertEquals("prod-rs", tol.get(0).get("value"));
        assertEquals("NoSchedule", tol.get(0).get("effect"));

        assertTrue(pod.containsKey("topologySpreadConstraints"));
    }

    @Test
    void deployment_name_is_sanitised_for_nodepool_name() {
        ComputeStrategy.Karpenter k = new ComputeStrategy.Karpenter(
                KarpenterSpec.sensibleAwsDefaults("x"));
        Map<String, Object> pod = new LinkedHashMap<>();
        String yaml = new KarpenterRenderer().render(k, "Mongo.Explorer_Dev", pod);
        assertTrue(yaml.contains("mex-mongo-explorer-dev"),
                "NodePool name must be DNS-1123 safe; got " + yaml);
    }

    @Test
    void spec_without_karpenter_spec_errors() {
        ComputeStrategy.Karpenter bareType = new ComputeStrategy.Karpenter();
        Map<String, Object> pod = new LinkedHashMap<>();
        assertThrows(IllegalArgumentException.class,
                () -> new KarpenterRenderer().render(bareType, "x", pod));
    }
}
