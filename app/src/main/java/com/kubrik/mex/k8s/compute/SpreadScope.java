package com.kubrik.mex.k8s.compute;

/**
 * v2.8.2 Q2.8.2-A — Topology-spread scope for a {@link
 * ComputeStrategy.NodePool}.
 *
 * <p>Decision 4 of milestone-v2.8.2.md: when the user targets a
 * dedicated pool, the {@code topologySpreadConstraints} selector
 * is narrowed to this deployment's pods only — so "spread across
 * nodes in the pool" becomes a per-deployment guarantee rather
 * than a namespace-wide one.</p>
 *
 * <p>v2.8.2 ships only {@link #WITHIN_POOL}; future releases may
 * add modes (e.g. cross-pool for multi-tier deployments).</p>
 */
public enum SpreadScope {
    WITHIN_POOL
}
