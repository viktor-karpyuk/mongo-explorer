package com.kubrik.mex.k8s.compute.karpenter;

import java.util.List;
import java.util.Objects;

/**
 * v2.8.3 Q2.8.3-A — Karpenter-strategy sub-record carrying the
 * NodePool wizard inputs.
 *
 * <p>Mirrors the JSON shape in milestone-v2.8.3.md §3:</p>
 * <pre>{@code
 *   { "type": "karpenter",
 *     "node_class_ref": {
 *       "api_version": "karpenter.k8s.aws/v1", "kind": "EC2NodeClass",
 *       "name": "default"
 *     },
 *     "capacity_type": ["spot", "on-demand"],
 *     "instance_families": ["m6i", "r6i"],
 *     "arch": ["amd64"],
 *     ... }
 * }</pre>
 *
 * <p>Lives outside the sealed {@link com.kubrik.mex.k8s.compute.ComputeStrategy}
 * hierarchy so the latter can keep its four-branch shape stable and
 * just expose a {@code spec()} accessor on the Karpenter branch.</p>
 */
public record KarpenterSpec(
        NodeClassRef nodeClassRef,
        List<String> capacityTypes,
        List<String> instanceFamilies,
        List<String> architectures,
        String cpuMin,
        String cpuMax,
        String memMin,
        String memMax,
        boolean consolidation,
        String expireAfter,
        String limitCpu,
        String limitMemory
) {
    public KarpenterSpec {
        Objects.requireNonNull(nodeClassRef, "nodeClassRef");
        capacityTypes = capacityTypes == null ? List.of() : List.copyOf(capacityTypes);
        instanceFamilies = instanceFamilies == null ? List.of() : List.copyOf(instanceFamilies);
        architectures = architectures == null ? List.of() : List.copyOf(architectures);
    }

    public record NodeClassRef(String apiVersion, String kind, String name) {
        public NodeClassRef {
            Objects.requireNonNull(apiVersion, "apiVersion");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(name, "name");
        }
    }

    public static KarpenterSpec sensibleAwsDefaults(String deploymentName) {
        return new KarpenterSpec(
                new NodeClassRef("karpenter.k8s.aws/v1", "EC2NodeClass", "default"),
                List.of("spot", "on-demand"),
                List.of("m6i", "r6i"),
                List.of("amd64"),
                "2", "32",
                "4Gi", "128Gi",
                true,
                "720h",
                "100", "400Gi");
    }

    /** True when the requirements list has at least one capacity type
     *  and one architecture — {@code PRE-KP-4}. */
    public boolean hasMinimumRequirements() {
        return !capacityTypes.isEmpty() && !architectures.isEmpty();
    }
}
