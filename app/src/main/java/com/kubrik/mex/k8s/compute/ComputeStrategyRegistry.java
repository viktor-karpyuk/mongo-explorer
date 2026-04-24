package com.kubrik.mex.k8s.compute;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * v2.8.2 Q2.8.2-A — Which {@link StrategyId}s are shipped / locked in
 * the current release.
 *
 * <p>The wizard's Dedicated-compute radio enumerates every id and
 * greys out ones not in {@link #shipped} with a *"Available in
 * v2.8.N"* label. This keeps the UI structure stable across the
 * v2.8.2 → v2.8.4 series; only the registry's {@code shipped} set
 * changes from release to release.</p>
 */
public final class ComputeStrategyRegistry {

    /** v2.8.2: None + NodePool are shipped. Karpenter / ManagedPool are
     *  greyed out in the radio. */
    public static final ComputeStrategyRegistry V2_8_2 = new ComputeStrategyRegistry(
            EnumSet.of(StrategyId.NONE, StrategyId.NODE_POOL));

    /** v2.8.3: adds Karpenter. */
    public static final ComputeStrategyRegistry V2_8_3 = new ComputeStrategyRegistry(
            EnumSet.of(StrategyId.NONE, StrategyId.NODE_POOL, StrategyId.KARPENTER));

    /** v2.8.4: adds ManagedPool → full radio unlocked. */
    public static final ComputeStrategyRegistry V2_8_4 = new ComputeStrategyRegistry(
            EnumSet.allOf(StrategyId.class));

    private final Set<StrategyId> shipped;

    public ComputeStrategyRegistry(Set<StrategyId> shipped) {
        if (!shipped.contains(StrategyId.NONE)) {
            throw new IllegalArgumentException(
                    "registry must always ship the NONE strategy as the v2.8.0-compatible default");
        }
        this.shipped = Set.copyOf(shipped);
    }

    public boolean isShipped(StrategyId id) {
        return shipped.contains(id);
    }

    public Set<StrategyId> shipped() { return shipped; }

    public List<StrategyId> allInRadioOrder() {
        return List.of(StrategyId.NONE, StrategyId.NODE_POOL,
                StrategyId.KARPENTER, StrategyId.MANAGED_POOL);
    }

    /** Copy of {@link #V2_8_2} for callers that want the current
     *  release's registry without hard-coding the constant name. */
    public static ComputeStrategyRegistry current() { return V2_8_2; }
}
