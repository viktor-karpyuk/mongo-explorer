package com.kubrik.mex.k8s.operator.psmdb;

import com.kubrik.mex.k8s.operator.DeploymentStatus;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PsmdbStatusParserTest {

    private final PsmdbStatusParser parser = new PsmdbStatusParser();

    @Test
    void ready_state_plus_ready_replset_is_ready() {
        Map<String, Object> status = Map.of(
                "state", "ready",
                "replsets", Map.of("rs0", Map.of("status", "ready")));
        assertEquals(DeploymentStatus.READY,
                parser.parse(status, List.of(), List.of()));
    }

    @Test
    void ready_but_one_replset_initializing_is_applying() {
        Map<String, Object> status = Map.of(
                "state", "ready",
                "replsets", Map.of(
                        "rs0", Map.of("status", "ready"),
                        "rs1", Map.of("status", "initializing")));
        assertEquals(DeploymentStatus.APPLYING,
                parser.parse(status, List.of(), List.of()));
    }

    @Test
    void sharded_requires_mongos_ready_too() {
        Map<String, Object> almost = Map.of(
                "state", "ready",
                "replsets", Map.of("rs0", Map.of("status", "ready")),
                "mongos", Map.of("status", "initializing"));
        assertEquals(DeploymentStatus.APPLYING,
                parser.parse(almost, List.of(), List.of()));

        Map<String, Object> done = Map.of(
                "state", "ready",
                "replsets", Map.of("rs0", Map.of("status", "ready")),
                "mongos", Map.of("status", "ready"));
        assertEquals(DeploymentStatus.READY,
                parser.parse(done, List.of(), List.of()));
    }

    @Test
    void error_state_is_failed() {
        assertEquals(DeploymentStatus.FAILED,
                parser.parse(Map.of("state", "error"), List.of(), List.of()));
    }

    @Test
    void initializing_is_applying() {
        assertEquals(DeploymentStatus.APPLYING,
                parser.parse(Map.of("state", "initializing"), List.of(), List.of()));
    }

    @Test
    void empty_status_is_unknown() {
        assertEquals(DeploymentStatus.UNKNOWN,
                parser.parse(null, List.of(), List.of()));
        assertEquals(DeploymentStatus.UNKNOWN,
                parser.parse(Map.of(), List.of(), List.of()));
    }

    @Test
    void crashloop_overrides_ready_state() {
        List<Map<String, Object>> pods = List.of(Map.of(
                "status", Map.of("containerStatuses", List.of(Map.of(
                        "restartCount", 5,
                        "state", Map.of("waiting",
                                Map.of("reason", "CrashLoopBackOff")))))));
        assertEquals(DeploymentStatus.FAILED,
                parser.parse(Map.of("state", "ready",
                        "replsets", Map.of("rs0", Map.of("status", "ready"))),
                        pods, List.of()));
    }

    @Test
    void case_insensitive_on_state() {
        assertEquals(DeploymentStatus.READY,
                parser.parse(Map.of(
                        "state", "READY",
                        "replsets", Map.of("rs0", Map.of("status", "READY"))),
                        List.of(), List.of()));
    }
}
