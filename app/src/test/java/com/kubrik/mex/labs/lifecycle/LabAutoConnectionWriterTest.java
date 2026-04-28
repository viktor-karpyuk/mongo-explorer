package com.kubrik.mex.labs.lifecycle;

import com.kubrik.mex.labs.model.LabDeployment;
import com.kubrik.mex.labs.model.LabStatus;
import com.kubrik.mex.labs.model.LabTemplate;
import com.kubrik.mex.labs.model.PortMap;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class LabAutoConnectionWriterTest {

    @Test
    void standalone_uri_is_direct_connection() {
        LabTemplate t = template("standalone", List.of("mongo"));
        LabDeployment d = deploy("mex-lab-standalone-abcd1234",
                Map.of("mongo", 27100));
        assertEquals("mongodb://127.0.0.1:27100/?directConnection=true",
                LabAutoConnectionWriter.composeUri(d, t));
    }

    @Test
    void rs_3_uri_includes_replSet_parameter() {
        LabTemplate t = template("rs-3", List.of("rs1a", "rs1b", "rs1c", "init"));
        LabDeployment d = deploy("mex-lab-rs-3-abcd1234",
                Map.of("rs1a", 27111, "rs1b", 27112, "rs1c", 27113, "init", 0));
        String uri = LabAutoConnectionWriter.composeUri(d, t);
        assertTrue(uri.contains("replicaSet=rs1"),
                "rs-3 URI must request replset discovery, got: " + uri);
        assertTrue(uri.contains("directConnection=false"));
        // Uses the rs1a seed port as the entry.
        assertTrue(uri.contains("127.0.0.1:27111"));
    }

    @Test
    void sharded_uri_targets_mongos_not_replset() {
        LabTemplate t = template("sharded-1", List.of("cfg1", "cfg2", "cfg3",
                "shard1a", "shard1b", "shard1c", "mongos", "init"));
        LabDeployment d = deploy("mex-lab-sharded-1-abcd1234",
                Map.of("cfg1", 27201, "cfg2", 27202, "cfg3", 27203,
                        "shard1a", 27211, "shard1b", 27212, "shard1c", 27213,
                        "mongos", 27200, "init", 0));
        String uri = LabAutoConnectionWriter.composeUri(d, t);
        assertTrue(uri.contains("127.0.0.1:27200"));
        assertFalse(uri.contains("replicaSet"),
                "sharded cluster connection must not request replset discovery");
    }

    /* ============================== fixtures ============================== */

    private static LabTemplate template(String id, List<String> containers) {
        return new LabTemplate(id, id, "", 100, 15, "mongo:latest",
                containers, "compose body with services:", Optional.empty(), 1);
    }

    private static LabDeployment deploy(String project, Map<String, Integer> ports) {
        return new LabDeployment(42L, "id", "v", "name", project,
                "/tmp/compose.yml", new PortMap(new java.util.LinkedHashMap<>(ports)),
                LabStatus.CREATING, false, false,
                1_700_000_000_000L, Optional.empty(), Optional.empty(),
                Optional.empty(), "mongo:latest", Optional.empty());
    }
}
