package com.kubrik.mex.k8s.discovery;

import com.kubrik.mex.k8s.model.DiscoveredMongo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PsmdbDiscovererTest {

    @Test
    void parses_unsharded_psmdb_cr() {
        Map<String, Object> root = Map.of("items", List.of(Map.of(
                "metadata", Map.of("name", "prod-cluster", "namespace", "mongo"),
                "spec", Map.of(
                        "image", "percona/percona-server-mongodb:7.0.8-5",
                        "replsets", List.of(Map.of("name", "rs0", "size", 3))),
                "status", Map.of("state", "ready"))));
        List<DiscoveredMongo> rows = PsmdbDiscoverer.parseList(root, 7L);
        assertEquals(1, rows.size());
        DiscoveredMongo d = rows.get(0);
        assertEquals(DiscoveredMongo.Origin.PSMDB, d.origin());
        assertEquals(DiscoveredMongo.Topology.RS3, d.topology());
        assertEquals("prod-cluster-rs0", d.serviceName().orElse(null));
        assertEquals(Boolean.TRUE, d.ready().orElse(null));
    }

    @Test
    void sharded_detected_and_uses_mongos_service() {
        Map<String, Object> root = Map.of("items", List.of(Map.of(
                "metadata", Map.of("name", "shards", "namespace", "mongo"),
                "spec", Map.of(
                        "sharding", Map.of("enabled", true),
                        "replsets", List.of(Map.of("name", "rs0", "size", 3)),
                        "image", "percona/percona-server-mongodb:7.0.8-5"))));
        DiscoveredMongo d = PsmdbDiscoverer.parseList(root, 1L).get(0);
        assertEquals(DiscoveredMongo.Topology.SHARDED, d.topology());
        assertEquals("shards-mongos", d.serviceName().orElse(null));
    }

    @Test
    void replset_size_fallthrough_to_unknown() {
        Map<String, Object> root = Map.of("items", List.of(Map.of(
                "metadata", Map.of("name", "n", "namespace", "n"),
                "spec", Map.of("replsets", List.of(Map.of("size", 7))))));
        assertEquals(DiscoveredMongo.Topology.UNKNOWN,
                PsmdbDiscoverer.parseList(root, 1L).get(0).topology());
    }

    @Test
    void missing_replsets_yields_unknown_topology() {
        Map<String, Object> root = Map.of("items", List.of(Map.of(
                "metadata", Map.of("name", "x", "namespace", "n"),
                "spec", Map.of())));
        assertEquals(DiscoveredMongo.Topology.UNKNOWN,
                PsmdbDiscoverer.parseList(root, 1L).get(0).topology());
    }
}
