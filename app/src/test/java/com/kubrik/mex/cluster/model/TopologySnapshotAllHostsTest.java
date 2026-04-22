package com.kubrik.mex.cluster.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-K follow-up — pins {@link TopologySnapshot#allHosts}, the
 * flattener the security-tab encryption + cert probes consume to
 * expand cluster-aggregate into per-node sweeps.
 */
class TopologySnapshotAllHostsTest {

    @Test
    void standalone_member_is_the_only_host() {
        TopologySnapshot snap = new TopologySnapshot(
                ClusterKind.STANDALONE, 0L, "7.0",
                List.of(Member.unknownAt("h1:27017")),
                List.of(), List.of(), List.of(), List.of());
        assertEquals(List.of("h1:27017"), snap.allHosts());
    }

    @Test
    void replset_surfaces_every_member() {
        TopologySnapshot snap = new TopologySnapshot(
                ClusterKind.REPLSET, 0L, "7.0",
                List.of(Member.unknownAt("rs0/h1:27017"),
                        Member.unknownAt("rs0/h2:27017"),
                        Member.unknownAt("rs0/h3:27017")),
                List.of(), List.of(), List.of(), List.of());
        assertEquals(3, snap.allHosts().size());
        assertTrue(snap.allHosts().contains("rs0/h1:27017"));
    }

    @Test
    void sharded_cluster_surfaces_shard_members_configservers_and_mongos() {
        TopologySnapshot snap = new TopologySnapshot(
                ClusterKind.SHARDED, 0L, "7.0",
                List.of(),
                List.of(new Shard("shard0", "shard0/h1:27018", false,
                                java.util.Map.of(),
                                List.of(Member.unknownAt("h1:27018"),
                                        Member.unknownAt("h2:27018"))),
                        new Shard("shard1", "shard1/h3:27018", false,
                                java.util.Map.of(),
                                List.of(Member.unknownAt("h3:27018"),
                                        Member.unknownAt("h4:27018")))),
                List.of(new Mongos("mongos1:27019", "7.0", 100L, null),
                        new Mongos("mongos2:27019", "7.0", 100L, null)),
                List.of(Member.unknownAt("cfg1:27020"),
                        Member.unknownAt("cfg2:27020"),
                        Member.unknownAt("cfg3:27020")),
                List.of());
        List<String> hosts = snap.allHosts();

        assertEquals(9, hosts.size(), "4 shard members + 2 mongos + 3 config");
        assertTrue(hosts.contains("h1:27018"));
        assertTrue(hosts.contains("h4:27018"));
        assertTrue(hosts.contains("mongos1:27019"));
        assertTrue(hosts.contains("cfg3:27020"));
    }

    @Test
    void duplicate_hosts_are_deduped() {
        // A host appearing both as a replset member and as a shard
        // member (unusual but possible during migrations) must only be
        // probed once.
        TopologySnapshot snap = new TopologySnapshot(
                ClusterKind.SHARDED, 0L, "7.0",
                List.of(Member.unknownAt("dup:27017")),
                List.of(new Shard("shard0", "shard0/dup:27017", false,
                        java.util.Map.of(),
                        List.of(Member.unknownAt("dup:27017")))),
                List.of(), List.of(), List.of());
        List<String> hosts = snap.allHosts();
        assertEquals(1, hosts.size());
        assertEquals("dup:27017", hosts.get(0));
    }

    @Test
    void empty_snapshot_returns_empty_list() {
        TopologySnapshot snap = new TopologySnapshot(
                ClusterKind.STANDALONE, 0L, "", List.of(),
                List.of(), List.of(), List.of(), List.of());
        assertTrue(snap.allHosts().isEmpty());
    }
}
