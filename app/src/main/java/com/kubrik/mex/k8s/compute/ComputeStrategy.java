package com.kubrik.mex.k8s.compute;

import com.kubrik.mex.k8s.compute.karpenter.KarpenterSpec;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.2 Q2.8.2-A — Sealed strategy for where Mongo pods get
 * scheduled. Spans the whole v2.8.2/3/4 series; v2.8.2 ships
 * {@link None} and {@link NodePool}, while {@link Karpenter} and
 * {@link ManagedPool} land as shipped strategies in v2.8.3 / v2.8.4
 * respectively. Records for the future strategies are kept in the
 * sealed hierarchy so the registry and wizard radio can enumerate
 * them without a parallel list of "coming soon" labels.
 *
 * <p>Serialisation to {@code provisioning_records.compute_strategy_json}
 * is handled by {@link ComputeStrategyJson}.</p>
 */
public sealed interface ComputeStrategy {

    /** Stable identifier used by {@link ComputeStrategyRegistry}
     *  lookup and JSON persistence. */
    StrategyId id();

    /** v2.8.0 behaviour — the cluster scheduler places Mongo pods.
     *  Default in every release. */
    record None() implements ComputeStrategy {
        @Override public StrategyId id() { return StrategyId.NONE; }
    }

    /** v2.8.2 — target an existing labelled/tainted pool. All three
     *  fields are required; the {@link SpreadScope} is fixed to
     *  {@link SpreadScope#WITHIN_POOL} for v2.8.2 but modelled as a
     *  parameter so later releases can widen the surface. */
    record NodePool(
            List<LabelPair> selector,
            List<Toleration> tolerations,
            SpreadScope spreadScope
    ) implements ComputeStrategy {
        public NodePool {
            Objects.requireNonNull(selector, "selector");
            Objects.requireNonNull(tolerations, "tolerations");
            Objects.requireNonNull(spreadScope, "spreadScope");
            if (selector.isEmpty()) {
                throw new IllegalArgumentException(
                        "NodePool selector must contain at least one label pair");
            }
            selector = List.copyOf(selector);
            tolerations = List.copyOf(tolerations);
        }
        @Override public StrategyId id() { return StrategyId.NODE_POOL; }
    }

    /** v2.8.3 — Karpenter NodePool rendered alongside the Mongo CR.
     *  The spec is optional so callers that only need the type tag
     *  (registry enumeration, UI greyed-out rendering) don't have to
     *  construct a full KarpenterSpec. */
    record Karpenter(Optional<KarpenterSpec> spec) implements ComputeStrategy {
        public Karpenter { spec = spec == null ? Optional.empty() : spec; }
        public Karpenter() { this(Optional.empty()); }
        public Karpenter(KarpenterSpec spec) { this(Optional.ofNullable(spec)); }
        @Override public StrategyId id() { return StrategyId.KARPENTER; }
    }

    /** v2.8.4 — Mongo Explorer creates a managed node-pool via the
     *  cloud API. Body populated when v2.8.4 lands. */
    record ManagedPool() implements ComputeStrategy {
        @Override public StrategyId id() { return StrategyId.MANAGED_POOL; }
    }

    ComputeStrategy NONE = new None();
}
