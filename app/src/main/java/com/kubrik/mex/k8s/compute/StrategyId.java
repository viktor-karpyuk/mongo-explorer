package com.kubrik.mex.k8s.compute;

/**
 * v2.8.2 Q2.8.2-A — Stable identifier for a {@link ComputeStrategy}.
 *
 * <p>Not a {@code ComputeStrategy.id()} method alone because the
 * wizard's strategy radio needs to enumerate all possible ids
 * (including those locked until a future release) without
 * constructing instances of the non-shipped records.</p>
 */
public enum StrategyId {
    /** v2.8.0 behaviour — the cluster scheduler places Mongo pods
     *  wherever it chooses. Available in every release. */
    NONE,
    /** v2.8.2 — caller picks an existing, pre-labelled, pre-tainted
     *  node pool. Mongo Explorer renders nodeSelector + tolerations
     *  + scoped topologySpread; pre-flight validates the pool. */
    NODE_POOL,
    /** v2.8.3 — Karpenter NodePool CR rendered alongside the Mongo
     *  CR. Karpenter handles VM lifecycle. */
    KARPENTER,
    /** v2.8.4 — Mongo Explorer calls the cloud's managed node-pool
     *  API (EKS first; GKE + AKS follow). */
    MANAGED_POOL
}
