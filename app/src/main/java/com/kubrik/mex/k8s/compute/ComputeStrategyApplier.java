package com.kubrik.mex.k8s.compute;

import com.kubrik.mex.k8s.compute.karpenter.KarpenterRenderer;
import com.kubrik.mex.k8s.compute.nodepool.NodePoolRenderer;

import java.util.Map;
import java.util.Optional;

/**
 * v2.8.3 Q2.8.3-A — Single entry point for operator renderers to
 * layer a {@link ComputeStrategy} onto their rendered pod spec.
 *
 * <p>Returns an {@link Optional} with the YAML of an extra manifest
 * that must be applied alongside the operator's CR when the strategy
 * needs one (v2.8.3's Karpenter {@code NodePool}). {@link
 * ComputeStrategy.None}, {@link ComputeStrategy.NodePool}, and
 * {@link ComputeStrategy.ManagedPool} all return empty — they either
 * do nothing or produce only in-pod mutations.</p>
 *
 * <p>Keeps the decision to "mutate pod and maybe emit aux manifest"
 * in one place so the MCO / PSMDB renderers stay operator-specific
 * and the compute branching stays strategy-specific.</p>
 */
public final class ComputeStrategyApplier {

    public record Result(Optional<String> extraManifest,
                          Optional<String> extraManifestKind,
                          Optional<String> extraManifestName) {
        public static Result none() {
            return new Result(Optional.empty(), Optional.empty(), Optional.empty());
        }
        public static Result manifest(String yaml, String kind, String name) {
            return new Result(Optional.of(yaml), Optional.of(kind), Optional.of(name));
        }
    }

    private ComputeStrategyApplier() {}

    /**
     * Mutate {@code podSpec} in place + decide whether an extra top-
     * level manifest is required.
     *
     * @param podSpec pod-spec map the operator renderer is assembling.
     * @param strategy the user's ComputeStrategy choice.
     * @param deploymentName Mongo deployment name — used for synthetic
     *                       labels + NodePool naming.
     */
    public static Result apply(Map<String, Object> podSpec,
                                 ComputeStrategy strategy,
                                 String deploymentName) {
        switch (strategy) {
            case ComputeStrategy.None n -> {
                return Result.none();
            }
            case ComputeStrategy.NodePool np -> {
                NodePoolRenderer.mutate(podSpec, np, deploymentName);
                return Result.none();
            }
            case ComputeStrategy.Karpenter k -> {
                if (k.spec().isEmpty()) return Result.none();
                String yaml = new KarpenterRenderer().render(k, deploymentName, podSpec);
                return Result.manifest(yaml, KarpenterRenderer.NODEPOOL_KIND,
                        "mex-" + safeName(deploymentName));
            }
            case ComputeStrategy.ManagedPool mp -> {
                // v2.8.4 populates this branch.
                return Result.none();
            }
        }
    }

    private static String safeName(String raw) {
        return raw.toLowerCase().replaceAll("[^a-z0-9-]", "-").replaceAll("-+", "-");
    }
}
