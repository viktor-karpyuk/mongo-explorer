package com.kubrik.mex.maint.param;

import com.kubrik.mex.maint.model.ClusterShape;
import com.kubrik.mex.maint.model.ParamProposal;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RecommenderTest {

    private final Recommender rec = new Recommender();

    @Test
    void big_oltp_box_recommends_higher_wt_limits() {
        ClusterShape shape = new ClusterShape("wiredTiger",
                128L * 1024L * 1024L * 1024L, 64, 500_000_000L,
                ClusterShape.Workload.OLTP, 7);
        List<ParamProposal> out = rec.recommend(shape, Map.of(
                "wiredTigerConcurrentReadTransactions", "128",
                "wiredTigerConcurrentWriteTransactions", "128"));
        ParamProposal reads = out.stream()
                .filter(p -> p.param().equals("wiredTigerConcurrentReadTransactions"))
                .findFirst().orElseThrow();
        assertEquals("256", reads.proposedValue());
        assertTrue(reads.isActionable());
    }

    @Test
    void small_box_recommends_lower_wt_read_limit() {
        ClusterShape shape = new ClusterShape("wiredTiger",
                4L * 1024L * 1024L * 1024L, 4, 1_000_000L,
                ClusterShape.Workload.MIXED, 6);
        List<ParamProposal> out = rec.recommend(shape, Map.of(
                "wiredTigerConcurrentReadTransactions", "128"));
        ParamProposal reads = out.stream()
                .filter(p -> p.param().equals("wiredTigerConcurrentReadTransactions"))
                .findFirst().orElseThrow();
        assertEquals("64", reads.proposedValue());
    }

    @Test
    void non_wiredTiger_engine_skips_wt_params() {
        ClusterShape shape = new ClusterShape("inMemory",
                16L * 1024L * 1024L * 1024L, 8, 100_000L,
                ClusterShape.Workload.OLTP, 7);
        List<ParamProposal> out = rec.recommend(shape, Map.of());
        assertTrue(out.stream().noneMatch(
                p -> p.param().startsWith("wiredTiger")));
    }

    @Test
    void exact_match_produces_INFO_severity() {
        ClusterShape shape = new ClusterShape("wiredTiger",
                16L * 1024L * 1024L * 1024L, 8, 1_000_000L,
                ClusterShape.Workload.MIXED, 7);
        List<ParamProposal> out = rec.recommend(shape, Map.of(
                "wiredTigerConcurrentReadTransactions", "128"));
        ParamProposal reads = out.stream()
                .filter(p -> p.param().equals("wiredTigerConcurrentReadTransactions"))
                .findFirst().orElseThrow();
        assertEquals(ParamProposal.Severity.INFO, reads.severity());
        assertFalse(reads.isActionable());
    }

    @Test
    void unknown_current_value_is_CONSIDER() {
        ClusterShape shape = new ClusterShape("wiredTiger",
                16L * 1024L * 1024L * 1024L, 8, 1_000_000L,
                ClusterShape.Workload.MIXED, 7);
        // No current value supplied → "?" placeholder → CONSIDER.
        List<ParamProposal> out = rec.recommend(shape, Map.of());
        ParamProposal reads = out.stream()
                .filter(p -> p.param().equals("wiredTigerConcurrentReadTransactions"))
                .findFirst().orElseThrow();
        assertEquals(ParamProposal.Severity.CONSIDER, reads.severity());
    }

    @Test
    void oltp_recommends_notablescan_true() {
        ClusterShape shape = new ClusterShape("wiredTiger",
                32L * 1024L * 1024L * 1024L, 16, 10_000_000L,
                ClusterShape.Workload.OLTP, 7);
        ParamProposal nts = rec.recommend(shape,
                Map.of("notablescan", "false"))
                .stream().filter(p -> p.param().equals("notablescan"))
                .findFirst().orElseThrow();
        assertEquals("true", nts.proposedValue());
    }

    @Test
    void olap_recommends_notablescan_false() {
        ClusterShape shape = new ClusterShape("wiredTiger",
                256L * 1024L * 1024L * 1024L, 64, 10_000_000_000L,
                ClusterShape.Workload.OLAP, 7);
        ParamProposal nts = rec.recommend(shape,
                Map.of("notablescan", "false"))
                .stream().filter(p -> p.param().equals("notablescan"))
                .findFirst().orElseThrow();
        assertEquals("false", nts.proposedValue());
    }
}
