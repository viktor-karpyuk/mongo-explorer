package com.kubrik.mex.cluster;

import com.kubrik.mex.cluster.model.ClusterKind;
import com.kubrik.mex.cluster.model.Member;
import com.kubrik.mex.cluster.model.MemberState;
import com.kubrik.mex.cluster.model.Shard;
import com.kubrik.mex.cluster.model.TopologySnapshot;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.4 TOPO-10..14 — canonical JSON + sha256 stability.
 */
class TopologySnapshotSerializerTest {

    @Test
    void equalSnapshotsProduceEqualHash() {
        TopologySnapshot a = replSet(1_000L);
        TopologySnapshot b = replSet(1_000L);
        assertEquals(a.sha256(), b.sha256());
        assertEquals(a.toCanonicalJson(), b.toCanonicalJson());
    }

    @Test
    void memberOrderDoesNotAffectHash() {
        TopologySnapshot reverse = new TopologySnapshot(
                ClusterKind.REPLSET, 1_000L, "7.0.5",
                List.of(secondary("b:27018"), primary("a:27018"), secondary("c:27018")),
                List.of(), List.of(), List.of(), List.of());
        TopologySnapshot ordered = replSet(1_000L);
        assertEquals(ordered.sha256(), reverse.sha256(),
                "member list order must not alter canonical hash");
    }

    @Test
    void capturedAtChangesHashButStructuralEqualityHolds() {
        TopologySnapshot a = replSet(1_000L);
        TopologySnapshot b = replSet(2_000L);
        assertNotEquals(a.sha256(), b.sha256());
        assertEquals(a.structuralCanonicalJson(), b.structuralCanonicalJson());
    }

    @Test
    void stateChangeChangesHash() {
        TopologySnapshot a = replSet(1_000L);
        TopologySnapshot b = new TopologySnapshot(
                ClusterKind.REPLSET, 1_000L, "7.0.5",
                List.of(primary("a:27018"), secondary("b:27018"),
                        new Member("c:27018", MemberState.DOWN, 1, 1, false, false,
                                Map.of(), null, null, null, null, null, null)),
                List.of(), List.of(), List.of(), List.of());
        assertNotEquals(a.sha256(), b.sha256(),
                "state transition from SECONDARY to DOWN must alter canonical hash");
    }

    @Test
    void shardedSnapshotHashIsDeterministic() {
        TopologySnapshot sharded = new TopologySnapshot(
                ClusterKind.SHARDED, 10_000L, "7.0.5",
                List.of(), List.of(
                        new Shard("shardB", "shardB/b1:27018,b2:27018", false, Map.of("tier", "b"), List.of()),
                        new Shard("shardA", "shardA/a1:27018,a2:27018", false, Map.of("tier", "a"), List.of())),
                List.of(), List.of(), List.of());
        TopologySnapshot sameOrder = new TopologySnapshot(
                ClusterKind.SHARDED, 10_000L, "7.0.5",
                List.of(), List.of(
                        new Shard("shardA", "shardA/a1:27018,a2:27018", false, Map.of("tier", "a"), List.of()),
                        new Shard("shardB", "shardB/b1:27018,b2:27018", false, Map.of("tier", "b"), List.of())),
                List.of(), List.of(), List.of());
        assertEquals(sharded.sha256(), sameOrder.sha256(),
                "shard list order must not alter canonical hash");
    }

    @Test
    void majorMinorParses() {
        TopologySnapshot s = replSet(1L);
        assertArrayEquals(new int[]{7, 0}, s.majorMinor());
    }

    @Test
    void memberCountDerivesFromMembersOrShards() {
        TopologySnapshot rs = replSet(1L);
        assertEquals(3, rs.memberCount());
        assertEquals(0, rs.shardCount());

        TopologySnapshot sharded = new TopologySnapshot(
                ClusterKind.SHARDED, 1L, "7.0.5", List.of(),
                List.of(new Shard("a", "", false, Map.of(),
                                List.of(primary("a1:27018"), secondary("a2:27018"))),
                        new Shard("b", "", false, Map.of(),
                                List.of(primary("b1:27018")))),
                List.of(), List.of(), List.of());
        assertEquals(3, sharded.memberCount());
        assertEquals(2, sharded.shardCount());
    }

    /* ---- fixtures ---- */

    private static TopologySnapshot replSet(long capturedAt) {
        return new TopologySnapshot(
                ClusterKind.REPLSET, capturedAt, "7.0.5",
                List.of(primary("a:27018"), secondary("b:27018"), secondary("c:27018")),
                List.of(), List.of(), List.of(), List.of());
    }

    private static Member primary(String host) {
        return new Member(host, MemberState.PRIMARY, 1, 1, false, false,
                Map.of(), 100L, null, null, 3600L, null, 7);
    }

    private static Member secondary(String host) {
        return new Member(host, MemberState.SECONDARY, 1, 1, false, false,
                Map.of(), 90L, 10L, 1L, 3600L, "a:27018", 7);
    }
}
