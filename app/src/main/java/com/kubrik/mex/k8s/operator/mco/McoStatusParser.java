package com.kubrik.mex.k8s.operator.mco;

import com.kubrik.mex.k8s.operator.DeploymentStatus;

import java.util.List;
import java.util.Map;

/**
 * v2.8.1 Q2.8.1-E3 — Maps a live {@code MongoDBCommunity.status}
 * subresource (+ pod list + event list) into the unified
 * {@link DeploymentStatus}.
 *
 * <p>Decision tree:</p>
 * <ul>
 *   <li>{@code status.phase = "Running"} + every {@code status.members[*].state}
 *       in {PRIMARY, SECONDARY, ARBITER} → {@link DeploymentStatus#READY}.</li>
 *   <li>{@code status.phase = "Failed"} → {@link DeploymentStatus#FAILED}.</li>
 *   <li>Any pod in {@code CrashLoopBackOff} with {@code restartCount ≥ 3}
 *       → {@link DeploymentStatus#FAILED}.</li>
 *   <li>Empty or {@code Pending} phase → {@link DeploymentStatus#APPLYING}.</li>
 *   <li>Otherwise → {@link DeploymentStatus#UNKNOWN}.</li>
 * </ul>
 *
 * <p>Inputs are generic {@link Map}s instead of the generated CR
 * stubs so the parser stays tolerant of operator-version drift —
 * a new status subfield won't break this class.</p>
 */
public final class McoStatusParser {

    public DeploymentStatus parse(Map<String, Object> crStatus,
                                    List<Map<String, Object>> pods,
                                    List<Map<String, Object>> events) {
        if (crashLoopDetected(pods)) return DeploymentStatus.FAILED;
        if (crStatus == null || crStatus.isEmpty()) return DeploymentStatus.UNKNOWN;

        String phase = asString(crStatus.get("phase"));
        if (phase == null) return DeploymentStatus.UNKNOWN;
        return switch (phase) {
            case "Running" -> allMembersHealthy(crStatus)
                    ? DeploymentStatus.READY
                    : DeploymentStatus.APPLYING;
            case "Failed" -> DeploymentStatus.FAILED;
            case "Pending", "" -> DeploymentStatus.APPLYING;
            default -> DeploymentStatus.UNKNOWN;
        };
    }

    static boolean allMembersHealthy(Map<String, Object> crStatus) {
        Object raw = crStatus.get("members");
        if (!(raw instanceof List<?> members) || members.isEmpty()) return false;
        for (Object m : members) {
            if (!(m instanceof Map<?, ?> member)) return false;
            String state = asString(member.get("state"));
            if (state == null) return false;
            if (!(state.equals("PRIMARY") || state.equals("SECONDARY") || state.equals("ARBITER"))) {
                return false;
            }
        }
        return true;
    }

    static boolean crashLoopDetected(List<Map<String, Object>> pods) {
        if (pods == null) return false;
        for (Map<String, Object> pod : pods) {
            Object statusRaw = pod.get("status");
            if (!(statusRaw instanceof Map<?, ?> status)) continue;
            Object containerStatuses = status.get("containerStatuses");
            if (!(containerStatuses instanceof List<?> list)) continue;
            for (Object cs : list) {
                if (!(cs instanceof Map<?, ?> container)) continue;
                Object stateRaw = container.get("state");
                if (!(stateRaw instanceof Map<?, ?> state)) continue;
                Object waitingRaw = state.get("waiting");
                if (!(waitingRaw instanceof Map<?, ?> waiting)) continue;
                if ("CrashLoopBackOff".equals(asString(waiting.get("reason")))) {
                    int restarts = asInt(container.get("restartCount"), 0);
                    if (restarts >= 3) return true;
                }
            }
        }
        return false;
    }

    private static String asString(Object o) { return o == null ? null : o.toString(); }

    private static int asInt(Object o, int dflt) {
        if (o instanceof Number n) return n.intValue();
        if (o instanceof String s) {
            try { return Integer.parseInt(s); } catch (NumberFormatException ignored) {}
        }
        return dflt;
    }
}
