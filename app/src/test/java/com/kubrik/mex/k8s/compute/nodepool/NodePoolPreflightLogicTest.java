package com.kubrik.mex.k8s.compute.nodepool;

import com.kubrik.mex.k8s.compute.LabelPair;
import com.kubrik.mex.k8s.compute.Toleration;
import io.kubernetes.client.openapi.models.V1Taint;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.2 Q2.8.2-C — Unit coverage of the pure logic the node-pool
 * preflight checks rely on. The API-touching branches are left to
 * integration tests against a live kind cluster.
 */
class NodePoolPreflightLogicTest {

    @Test
    void label_selector_is_and_match_all_keys_required() {
        List<LabelPair> sel = List.of(
                new LabelPair("workload", "mongodb"),
                new LabelPair("tier", "prod"));
        assertTrue(NodePoolLookupService.matches(
                Map.of("workload", "mongodb", "tier", "prod", "extra", "x"), sel));
        assertFalse(NodePoolLookupService.matches(
                Map.of("workload", "mongodb"), sel), "missing tier must fail");
        assertFalse(NodePoolLookupService.matches(
                Map.of("workload", "mongodb", "tier", "dev"), sel),
                "mismatched value must fail");
    }

    @Test
    void empty_selector_matches_every_node() {
        assertTrue(NodePoolLookupService.matches(Map.of(), List.of()));
        assertTrue(NodePoolLookupService.matches(Map.of("x", "y"), List.of()));
    }

    @Test
    void exact_taint_with_value_is_tolerated_by_equal_operator() {
        V1Taint taint = new V1Taint().key("dedicated").value("mongo").effect("NoSchedule");
        assertTrue(NodePoolPreflightChecks.TaintsCheck.isTolerated(
                taint, List.of(new Toleration("dedicated", "mongo", Toleration.Effect.NO_SCHEDULE))));
    }

    @Test
    void exists_toleration_matches_any_taint_value_for_the_key() {
        V1Taint taint = new V1Taint().key("gpu").value("nvidia-a100").effect("NoSchedule");
        assertTrue(NodePoolPreflightChecks.TaintsCheck.isTolerated(
                taint, List.of(new Toleration("gpu", null, Toleration.Effect.NO_SCHEDULE))));
    }

    @Test
    void effect_mismatch_is_not_tolerated() {
        V1Taint taint = new V1Taint().key("k").value("v").effect("NoSchedule");
        assertFalse(NodePoolPreflightChecks.TaintsCheck.isTolerated(
                taint, List.of(new Toleration("k", "v", Toleration.Effect.NO_EXECUTE))));
    }

    @Test
    void value_mismatch_is_not_tolerated_when_toleration_is_equal_shape() {
        V1Taint taint = new V1Taint().key("k").value("real").effect("NoSchedule");
        assertFalse(NodePoolPreflightChecks.TaintsCheck.isTolerated(
                taint, List.of(new Toleration("k", "other", Toleration.Effect.NO_SCHEDULE))));
    }

    @Test
    void every_check_id_is_stable_and_unique() {
        var ids = NodePoolPreflightChecks.all().stream().map(c -> c.id()).toList();
        assertEquals(4, ids.size());
        assertEquals(4, ids.stream().distinct().count(), "ids must not collide");
        for (String id : ids) assertTrue(id.startsWith("preflight.node-pool."));
    }
}
