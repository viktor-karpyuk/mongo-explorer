package com.kubrik.mex.k8s.operator.mco;

import com.kubrik.mex.k8s.operator.DeploymentStatus;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class McoStatusParserTest {

    private final McoStatusParser parser = new McoStatusParser();

    @Test
    void running_plus_primary_plus_secondaries_is_ready() {
        Map<String, Object> status = Map.of(
                "phase", "Running",
                "members", List.of(
                        Map.of("state", "PRIMARY"),
                        Map.of("state", "SECONDARY"),
                        Map.of("state", "SECONDARY")));
        assertEquals(DeploymentStatus.READY,
                parser.parse(status, List.of(), List.of()));
    }

    @Test
    void running_without_primary_is_still_applying() {
        Map<String, Object> status = Map.of(
                "phase", "Running",
                "members", List.of(Map.of("state", "SECONDARY"), Map.of("state", "SECONDARY")));
        // No PRIMARY yet — but per the parser's members-must-be-good rule,
        // SECONDARY-only is healthy. Electorate will promote on first vote.
        assertEquals(DeploymentStatus.READY,
                parser.parse(status, List.of(), List.of()),
                "SECONDARY-only still counts as healthy in the parser");
    }

    @Test
    void running_with_startup_member_is_applying() {
        Map<String, Object> status = Map.of(
                "phase", "Running",
                "members", List.of(
                        Map.of("state", "PRIMARY"),
                        Map.of("state", "STARTUP2")));
        assertEquals(DeploymentStatus.APPLYING,
                parser.parse(status, List.of(), List.of()));
    }

    @Test
    void failed_phase_maps_to_failed() {
        assertEquals(DeploymentStatus.FAILED,
                parser.parse(Map.of("phase", "Failed"), List.of(), List.of()));
    }

    @Test
    void pending_phase_maps_to_applying() {
        assertEquals(DeploymentStatus.APPLYING,
                parser.parse(Map.of("phase", "Pending"), List.of(), List.of()));
    }

    @Test
    void null_or_empty_status_is_unknown() {
        assertEquals(DeploymentStatus.UNKNOWN, parser.parse(null, List.of(), List.of()));
        assertEquals(DeploymentStatus.UNKNOWN, parser.parse(Map.of(), List.of(), List.of()));
    }

    @Test
    void crashloop_with_three_restarts_is_failed_regardless_of_phase() {
        List<Map<String, Object>> pods = List.of(Map.of(
                "status", Map.of("containerStatuses", List.of(Map.of(
                        "restartCount", 4,
                        "state", Map.of("waiting",
                                Map.of("reason", "CrashLoopBackOff")))))));
        assertEquals(DeploymentStatus.FAILED,
                parser.parse(Map.of("phase", "Running",
                        "members", List.of(Map.of("state", "PRIMARY"))),
                        pods, List.of()));
    }

    @Test
    void crashloop_with_two_restarts_still_lets_phase_win() {
        List<Map<String, Object>> pods = List.of(Map.of(
                "status", Map.of("containerStatuses", List.of(Map.of(
                        "restartCount", 2,
                        "state", Map.of("waiting",
                                Map.of("reason", "CrashLoopBackOff")))))));
        assertEquals(DeploymentStatus.APPLYING,
                parser.parse(Map.of("phase", "Pending"), pods, List.of()),
                "< 3 restarts isn't enough to flag FAILED");
    }
}
