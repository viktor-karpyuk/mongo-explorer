package com.kubrik.mex.k8s.discovery;

import com.kubrik.mex.k8s.model.DiscoveredMongo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class McoDiscovererTest {

    @Test
    void parses_minimal_mco_cr() {
        Map<String, Object> root = Map.of("items", List.of(Map.of(
                "metadata", Map.of("name", "my-rs", "namespace", "mongo"),
                "spec", Map.of("members", 3, "version", "7.0.9"),
                "status", Map.of("phase", "Running"))));
        List<DiscoveredMongo> rows = McoDiscoverer.parseList(root, 42L);
        assertEquals(1, rows.size());
        DiscoveredMongo d = rows.get(0);
        assertEquals("my-rs", d.name());
        assertEquals("mongo", d.namespace());
        assertEquals(DiscoveredMongo.Topology.RS3, d.topology());
        assertEquals(DiscoveredMongo.Origin.MCO, d.origin());
        assertEquals("7.0.9", d.mongoVersion().orElse(null));
        assertEquals(Boolean.TRUE, d.ready().orElse(null));
    }

    @Test
    void detects_standalone_and_rs5() {
        Map<String, Object> root = Map.of("items", List.of(
                Map.of("metadata", Map.of("name", "solo", "namespace", "a"),
                        "spec", Map.of("members", 1)),
                Map.of("metadata", Map.of("name", "big-rs", "namespace", "a"),
                        "spec", Map.of("members", 5))));
        List<DiscoveredMongo> rows = McoDiscoverer.parseList(root, 1L);
        assertEquals(DiscoveredMongo.Topology.STANDALONE, rows.get(0).topology());
        assertEquals(DiscoveredMongo.Topology.RS5, rows.get(1).topology());
    }

    @Test
    void unknown_for_weird_member_count() {
        Map<String, Object> root = Map.of("items", List.of(Map.of(
                "metadata", Map.of("name", "x", "namespace", "a"),
                "spec", Map.of("members", 7))));
        assertEquals(DiscoveredMongo.Topology.UNKNOWN,
                McoDiscoverer.parseList(root, 1L).get(0).topology());
    }

    @Test
    void auth_mode_detection_defaults_to_scram() {
        assertEquals(DiscoveredMongo.AuthKind.SCRAM,
                McoDiscoverer.parseMcoAuth(null));
        assertEquals(DiscoveredMongo.AuthKind.SCRAM,
                McoDiscoverer.parseMcoAuth(Map.of()));
        assertEquals(DiscoveredMongo.AuthKind.SCRAM,
                McoDiscoverer.parseMcoAuth(Map.of("modes", List.of("SCRAM"))));
        assertEquals(DiscoveredMongo.AuthKind.X509,
                McoDiscoverer.parseMcoAuth(Map.of("modes", List.of("X509"))));
    }

    @Test
    void empty_items_yields_empty_list() {
        assertTrue(McoDiscoverer.parseList(Map.of("items", List.of()), 1L).isEmpty());
        assertTrue(McoDiscoverer.parseList(Map.of(), 1L).isEmpty());
        assertTrue(McoDiscoverer.parseList(null, 1L).isEmpty());
    }

    @Test
    void missing_metadata_is_skipped_not_thrown() {
        Map<String, Object> root = Map.of("items", List.of(
                Map.of("spec", Map.of("members", 3)),       // no metadata
                Map.of("metadata", Map.of("namespace", "a"))  // no name
        ));
        assertTrue(McoDiscoverer.parseList(root, 1L).isEmpty());
    }
}
