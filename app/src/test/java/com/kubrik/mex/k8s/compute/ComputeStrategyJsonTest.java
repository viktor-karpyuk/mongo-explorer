package com.kubrik.mex.k8s.compute;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.2 Q2.8.2-A — Round-trip + forward-compat invariants for the
 * {@code provisioning_records.compute_strategy_json} codec.
 */
class ComputeStrategyJsonTest {

    @Test
    void none_serialises_as_null() {
        assertNull(ComputeStrategyJson.toJson(ComputeStrategy.NONE));
        assertNull(ComputeStrategyJson.toJson(new ComputeStrategy.None()));
    }

    @Test
    void null_json_decodes_to_none() {
        assertEquals(ComputeStrategy.NONE, ComputeStrategyJson.fromJson(null));
        assertEquals(ComputeStrategy.NONE, ComputeStrategyJson.fromJson(""));
        assertEquals(ComputeStrategy.NONE, ComputeStrategyJson.fromJson("   "));
    }

    @Test
    void node_pool_round_trips_preserving_selector_and_tolerations() {
        ComputeStrategy.NodePool original = new ComputeStrategy.NodePool(
                List.of(new LabelPair("workload", "mongodb"),
                        new LabelPair("tier", "prod")),
                List.of(new Toleration("dedicated", "mongo", Toleration.Effect.NO_SCHEDULE),
                        new Toleration("nosafe", null, Toleration.Effect.NO_EXECUTE)),
                SpreadScope.WITHIN_POOL);

        String json = ComputeStrategyJson.toJson(original);
        assertNotNull(json);
        assertTrue(json.contains("node_pool"));
        assertTrue(json.contains("workload"));
        assertTrue(json.contains("NoSchedule"));
        assertTrue(json.contains("NoExecute"));

        ComputeStrategy round = ComputeStrategyJson.fromJson(json);
        assertInstanceOf(ComputeStrategy.NodePool.class, round);
        ComputeStrategy.NodePool back = (ComputeStrategy.NodePool) round;
        assertEquals(2, back.selector().size());
        assertTrue(back.selector().contains(new LabelPair("workload", "mongodb")));
        assertEquals(2, back.tolerations().size());
        assertEquals(Toleration.Effect.NO_EXECUTE, back.tolerations().get(1).effect());
        assertEquals(SpreadScope.WITHIN_POOL, back.spreadScope());
    }

    @Test
    void empty_selector_rejected_at_construction() {
        assertThrows(IllegalArgumentException.class,
                () -> new ComputeStrategy.NodePool(
                        List.of(), List.of(), SpreadScope.WITHIN_POOL));
    }

    @Test
    void unknown_type_forward_compatible_decodes_to_none() {
        // A provisioning_records row written by a later Mongo Explorer
        // version must not crash this version's DAO.
        ComputeStrategy s = ComputeStrategyJson.fromJson(
                "{\"type\":\"gravitational-field-manipulator\"}");
        assertEquals(ComputeStrategy.NONE, s);
    }

    @Test
    void karpenter_placeholder_round_trips_even_though_body_is_empty() {
        String json = ComputeStrategyJson.toJson(new ComputeStrategy.Karpenter());
        assertNotNull(json);
        assertTrue(json.contains("karpenter"));
        assertInstanceOf(ComputeStrategy.Karpenter.class, ComputeStrategyJson.fromJson(json));
    }

    @Test
    void managed_pool_placeholder_round_trips_even_though_body_is_empty() {
        String json = ComputeStrategyJson.toJson(new ComputeStrategy.ManagedPool());
        assertNotNull(json);
        assertTrue(json.contains("managed_pool"));
        assertInstanceOf(ComputeStrategy.ManagedPool.class, ComputeStrategyJson.fromJson(json));
    }

    @Test
    void registry_locks_future_strategies_per_release() {
        ComputeStrategyRegistry v282 = ComputeStrategyRegistry.V2_8_2;
        assertTrue(v282.isShipped(StrategyId.NONE));
        assertTrue(v282.isShipped(StrategyId.NODE_POOL));
        assertFalse(v282.isShipped(StrategyId.KARPENTER));
        assertFalse(v282.isShipped(StrategyId.MANAGED_POOL));

        ComputeStrategyRegistry v283 = ComputeStrategyRegistry.V2_8_3;
        assertTrue(v283.isShipped(StrategyId.KARPENTER));
        assertFalse(v283.isShipped(StrategyId.MANAGED_POOL));

        ComputeStrategyRegistry v284 = ComputeStrategyRegistry.V2_8_4;
        for (StrategyId id : StrategyId.values()) {
            assertTrue(v284.isShipped(id), id + " should be shipped in v2.8.4");
        }
    }
}
