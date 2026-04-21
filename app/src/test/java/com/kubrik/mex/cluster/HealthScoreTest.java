package com.kubrik.mex.cluster;

import com.kubrik.mex.cluster.model.ClusterKind;
import com.kubrik.mex.cluster.model.HealthScore;
import com.kubrik.mex.cluster.model.Member;
import com.kubrik.mex.cluster.model.MemberState;
import com.kubrik.mex.cluster.model.Shard;
import com.kubrik.mex.cluster.model.TopologySnapshot;
import com.kubrik.mex.cluster.service.HealthScorer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HealthScoreTest {

    @Test
    void fullHealthReplicaSetHits100() {
        HealthScore s = HealthScorer.score(replSet(
                primary("a:1", 0L), secondary("b:1", 100L), secondary("c:1", 50L)));
        assertEquals(100, s.score());
        assertEquals(HealthScore.Band.GREEN, s.band());
        assertTrue(s.negatives().isEmpty());
    }

    @Test
    void missingPrimaryDrops40() {
        HealthScore s = HealthScorer.score(replSet(
                secondary("a:1", 100L), secondary("b:1", 100L), secondary("c:1", 100L)));
        assertEquals(60, s.score(), "−40 primary");
        assertTrue(s.negatives().stream().anyMatch(n -> n.contains("No primary")));
    }

    @Test
    void quorumBelowMajorityDrops30() {
        HealthScore s = HealthScorer.score(replSet(
                primary("a:1", 0L),
                memberDown("b:1"),
                memberDown("c:1")));
        // primary present = 40, quorum = 0 (1 of 3 voters healthy), lag OK (no secondaries) = 15,
        // version OK = 10, no shards = 5 → 70
        assertEquals(70, s.score());
        assertEquals(HealthScore.Band.AMBER, s.band());
    }

    @Test
    void laggingSecondaryDrops15() {
        HealthScore s = HealthScorer.score(replSet(
                primary("a:1", 0L), secondary("b:1", 40_000L), secondary("c:1", 50L)));
        assertEquals(85, s.score(), "−15 lag");
        assertTrue(s.negatives().stream().anyMatch(n -> n.contains("40 s behind")));
    }

    @Test
    void missingVersionDrops10() {
        TopologySnapshot noVersion = new TopologySnapshot(
                ClusterKind.REPLSET, 1L, "",
                List.of(primary("a:1", 0L), secondary("b:1", 100L), secondary("c:1", 100L)),
                List.of(), List.of(), List.of(), List.of());
        HealthScore s = HealthScorer.score(noVersion);
        assertEquals(90, s.score());
        assertTrue(s.negatives().stream().anyMatch(n -> n.contains("version unreported")));
    }

    @Test
    void unreachableShardDrops5() {
        TopologySnapshot snap = new TopologySnapshot(
                ClusterKind.SHARDED, 1L, "7.0.5",
                List.of(), List.of(
                        new Shard("a", "a/a1:1", false, Map.of(),
                                List.of(primary("a1:1", 0L), secondary("a2:1", 100L))),
                        new Shard("b", "b/b1:1", false, Map.of(),
                                List.of(memberDown("b1:1"), memberDown("b2:1")))),
                List.of(), List.of(), List.of());
        HealthScore s = HealthScorer.score(snap);
        assertTrue(s.negatives().stream().anyMatch(n -> n.contains("Shard b has no reachable members.")),
                "shard b should be flagged as unreachable");
        assertTrue(s.score() <= 95);
    }

    @Test
    void standaloneDegenerate() {
        TopologySnapshot up = new TopologySnapshot(
                ClusterKind.STANDALONE, 1L, "7.0.5",
                List.of(new Member("a:1", MemberState.PRIMARY, null, null, null, null,
                        Map.of(), null, null, null, null, null, null)),
                List.of(), List.of(), List.of(), List.of());
        assertEquals(100, HealthScorer.score(up).score());

        TopologySnapshot down = new TopologySnapshot(
                ClusterKind.STANDALONE, 1L, "7.0.5",
                List.of(new Member("a:1", MemberState.DOWN, null, null, null, null,
                        Map.of(), null, null, null, null, null, null)),
                List.of(), List.of(), List.of(), List.of());
        assertEquals(0, HealthScorer.score(down).score());
    }

    @Test
    void nullSnapshotYieldsZero() {
        HealthScore s = HealthScorer.score(null);
        assertEquals(0, s.score());
        assertEquals(HealthScore.Band.RED, s.band());
    }

    @Test
    void bandThresholdsMatchSpec() {
        assertEquals(HealthScore.Band.GREEN, HealthScore.Band.forScore(100));
        assertEquals(HealthScore.Band.GREEN, HealthScore.Band.forScore(90));
        assertEquals(HealthScore.Band.AMBER, HealthScore.Band.forScore(89));
        assertEquals(HealthScore.Band.AMBER, HealthScore.Band.forScore(70));
        assertEquals(HealthScore.Band.RED,   HealthScore.Band.forScore(69));
        assertEquals(HealthScore.Band.RED,   HealthScore.Band.forScore(0));
    }

    /* -------- fixtures -------- */

    private static TopologySnapshot replSet(Member... members) {
        return new TopologySnapshot(ClusterKind.REPLSET, 1L, "7.0.5",
                List.of(members), List.of(), List.of(), List.of(), List.of());
    }

    private static Member primary(String host, long optimeMs) {
        return new Member(host, MemberState.PRIMARY, 1, 1, false, false,
                Map.of(), optimeMs, null, 1L, 3600L, null, 7);
    }

    private static Member secondary(String host, long lagMs) {
        return new Member(host, MemberState.SECONDARY, 1, 1, false, false,
                Map.of(), 0L, lagMs, 1L, 3600L, "a:1", 7);
    }

    private static Member memberDown(String host) {
        return new Member(host, MemberState.DOWN, 1, 1, false, false,
                Map.of(), null, null, null, null, null, 7);
    }
}
