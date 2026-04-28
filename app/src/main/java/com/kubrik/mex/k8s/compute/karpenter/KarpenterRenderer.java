package com.kubrik.mex.k8s.compute.karpenter;

import com.kubrik.mex.k8s.compute.ComputeStrategy;
import com.kubrik.mex.k8s.compute.LabelPair;
import com.kubrik.mex.k8s.compute.SpreadScope;
import com.kubrik.mex.k8s.compute.Toleration;
import com.kubrik.mex.k8s.compute.nodepool.NodePoolRenderer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * v2.8.3 Q2.8.3-A — Renders a Karpenter {@code NodePool} CR and
 * mutates the operator pod spec so Mongo pods land on nodes the
 * Karpenter controller provisions on demand.
 *
 * <p>Two outputs, both driven by a single {@link KarpenterSpec}:</p>
 * <ul>
 *   <li>A cluster-scoped {@code karpenter.sh/v1 NodePool} YAML
 *       document whose {@code requirements} encode the user's
 *       capacity-type / instance-family / architecture / range
 *       picks, whose {@code taints} includes a synthetic
 *       {@code mex.deployment=<name>:NoSchedule} so only our pods
 *       land on it, and whose {@code limits} cap the aggregate
 *       CPU + memory Karpenter will provision.</li>
 *   <li>Mutations to the Mongo pod spec (via {@link
 *       NodePoolRenderer#mutate}) that add the matching
 *       {@code nodeSelector} + {@code tolerations} + scoped
 *       {@code topologySpreadConstraints}.</li>
 * </ul>
 *
 * <p>Pure + deterministic: no clock reads, no randomness. Production
 * callers must hand the pod-spec map + {@link java.util.LinkedHashMap}s
 * so YAML byte-stability holds for the preview-hash invariant.</p>
 */
public final class KarpenterRenderer {

    public static final String NODEPOOL_API_VERSION = "karpenter.sh/v1";
    public static final String NODEPOOL_KIND = "NodePool";
    public static final String DEPLOYMENT_TAINT_KEY = "mex.deployment";

    /** Jackson YAML mapper construction is non-trivial (factory +
     *  module discovery + thread-local pool init); a static instance
     *  amortises that cost across every renderer call. ObjectMapper
     *  is documented thread-safe after configuration. */
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory()
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
            .disable(YAMLGenerator.Feature.SPLIT_LINES));

    /** Mutate {@code podSpec} so it selects + tolerates a Karpenter-
     *  managed NodePool owned by {@code deploymentName}. Visible as a
     *  standalone helper so PSMDB's multi-replset rendering can mutate
     *  every replset / configsvr / mongos pod spec without re-emitting
     *  the NodePool YAML per call. */
    public static void mutatePodForKarpenter(Map<String, Object> podSpec,
                                              String deploymentName,
                                              String labelSelectorName) {
        String nodePoolName = "mex-" + safeName(deploymentName);
        ComputeStrategy.NodePool bridge = new ComputeStrategy.NodePool(
                List.of(new LabelPair("karpenter.sh/nodepool", nodePoolName)),
                List.of(new Toleration(DEPLOYMENT_TAINT_KEY, deploymentName,
                        Toleration.Effect.NO_SCHEDULE)),
                SpreadScope.WITHIN_POOL);
        NodePoolRenderer.mutate(podSpec, bridge, labelSelectorName);
    }

    /** Emit the NodePool YAML + the matching pod mutations in a
     *  single pass. Caller supplies the mongo pod spec map, which is
     *  mutated in place. */
    public String render(ComputeStrategy.Karpenter strategy, String deploymentName,
                          Map<String, Object> podSpec) {
        KarpenterSpec sp = strategy.spec().orElseThrow(() ->
                new IllegalArgumentException("Karpenter strategy requires a KarpenterSpec"));

        String nodePoolName = "mex-" + safeName(deploymentName);
        String poolLabelValue = nodePoolName;

        // --- 1. Mutate the pod so it selects + tolerates the NodePool ---
        mutatePodForKarpenter(podSpec, deploymentName, deploymentName);

        // --- 2. Render the NodePool CR ---
        Map<String, Object> cr = new LinkedHashMap<>();
        cr.put("apiVersion", NODEPOOL_API_VERSION);
        cr.put("kind", NODEPOOL_KIND);
        cr.put("metadata", Map.of(
                "name", nodePoolName,
                "labels", Map.of(
                        "mex.provisioning/renderer", "mongo-explorer",
                        "mex.provisioning/deployment", deploymentName)));
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("template", templateBlock(sp, deploymentName, poolLabelValue));
        spec.put("disruption", disruptionBlock(sp));
        if (sp.limitCpu() != null || sp.limitMemory() != null) {
            Map<String, Object> limits = new LinkedHashMap<>();
            if (sp.limitCpu() != null) limits.put("cpu", sp.limitCpu());
            if (sp.limitMemory() != null) limits.put("memory", sp.limitMemory());
            spec.put("limits", limits);
        }
        cr.put("spec", spec);

        try { return YAML.writeValueAsString(cr); }
        catch (Exception e) { throw new IllegalStateException(
                "render karpenter nodepool: " + e.getMessage(), e); }
    }

    private Map<String, Object> templateBlock(KarpenterSpec sp, String deployment,
                                                String poolLabelValue) {
        Map<String, Object> template = new LinkedHashMap<>();
        template.put("metadata", Map.of("labels", Map.of(
                "karpenter.sh/nodepool", poolLabelValue)));

        Map<String, Object> tmplSpec = new LinkedHashMap<>();

        // nodeClassRef — shape changed between v1beta1 and v1; v1 uses
        // group + kind + name.
        Map<String, Object> ncRef = new LinkedHashMap<>();
        ncRef.put("group", groupOf(sp.nodeClassRef().apiVersion()));
        ncRef.put("kind", sp.nodeClassRef().kind());
        ncRef.put("name", sp.nodeClassRef().name());
        tmplSpec.put("nodeClassRef", ncRef);

        tmplSpec.put("requirements", requirements(sp));
        tmplSpec.put("taints", List.of(Map.of(
                "key", DEPLOYMENT_TAINT_KEY,
                "value", deployment,
                "effect", "NoSchedule")));

        template.put("spec", tmplSpec);
        return template;
    }

    private List<Map<String, Object>> requirements(KarpenterSpec sp) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (!sp.capacityTypes().isEmpty()) {
            out.add(Map.of(
                    "key", "karpenter.sh/capacity-type",
                    "operator", "In",
                    "values", sp.capacityTypes()));
        }
        if (!sp.instanceFamilies().isEmpty()) {
            out.add(Map.of(
                    "key", "karpenter.k8s.aws/instance-family",
                    "operator", "In",
                    "values", sp.instanceFamilies()));
        }
        if (!sp.architectures().isEmpty()) {
            out.add(Map.of(
                    "key", "kubernetes.io/arch",
                    "operator", "In",
                    "values", sp.architectures()));
        }
        return out;
    }

    private Map<String, Object> disruptionBlock(KarpenterSpec sp) {
        Map<String, Object> d = new LinkedHashMap<>();
        // Karpenter v1 (karpenter.sh/v1) renamed the consolidation
        // policy values: the legacy v1beta1 "WhenUnderutilized" is
        // now "WhenEmptyOrUnderutilized"; "WhenEmpty" is still valid.
        d.put("consolidationPolicy", sp.consolidation()
                ? "WhenEmptyOrUnderutilized" : "WhenEmpty");
        if (sp.expireAfter() != null) d.put("expireAfter", sp.expireAfter());
        return d;
    }

    /** Returns the API group portion of an apiVersion string —
     *  e.g. {@code "karpenter.k8s.aws/v1"} → {@code "karpenter.k8s.aws"}.
     *  The core API ({@code "v1"} alone) has an empty group, so a
     *  no-slash apiVersion returns {@code ""} per Kubernetes' wire
     *  shape (NodeClassRef.group is required-but-may-be-empty for
     *  core resources). */
    static String groupOf(String apiVersion) {
        if (apiVersion == null) return "";
        int slash = apiVersion.indexOf('/');
        return slash < 0 ? "" : apiVersion.substring(0, slash);
    }

    /** Sanitise a deployment name into a DNS-1123 subdomain. K8s
     *  requires [a-z0-9]([a-z0-9-]*[a-z0-9])? — must start and end
     *  with an alphanumeric. We always prefix with "mex-" upstream,
     *  so a leading-digit "123-rs" becomes "mex-123-rs" and keeps
     *  the start-letter rule. End-trim the trailing dash that the
     *  hyphen-collapse can produce ("foo-" → "foo"). */
    static String safeName(String raw) {
        if (raw == null) return "x";
        String squashed = raw.toLowerCase().replaceAll("[^a-z0-9-]", "-")
                .replaceAll("-+", "-")
                .replaceAll("^-|-$", "");
        if (squashed.isEmpty()) return "x";
        // DNS-1123 mandates a leading alphanumeric; the upstream
        // "mex-" prefix already satisfies this for the NodePool
        // name path. For other callers, prepend an "x" if needed.
        if (!Character.isLetter(squashed.charAt(0))
                && !Character.isDigit(squashed.charAt(0))) {
            squashed = "x" + squashed;
        }
        return squashed;
    }
}
