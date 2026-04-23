package com.kubrik.mex.k8s.operator.psmdb;

import com.kubrik.mex.k8s.operator.DeploymentStatus;

import java.util.List;
import java.util.Map;

/**
 * v2.8.1 Q2.8.1-F2 — PSMDB CR status projection.
 *
 * <p>PSMDB's status surface is richer than MCO's:</p>
 * <ul>
 *   <li>{@code status.state} (top-level) — ready / initializing /
 *       error / paused.</li>
 *   <li>{@code status.replsets[*]} — per-replset health.</li>
 *   <li>{@code status.mongos} — mongos deployment status (sharded only).</li>
 * </ul>
 *
 * <p>Decision tree: top-level {@code state = ready} and every
 * replset {@code .status = ready} (and {@code mongos.status = ready}
 * when sharded) → READY. {@code state = error} → FAILED. A
 * CrashLoopBackOff with restartCount ≥ 3 overrides to FAILED.
 * Otherwise → APPLYING or UNKNOWN.</p>
 */
public final class PsmdbStatusParser {

    public DeploymentStatus parse(Map<String, Object> crStatus,
                                    List<Map<String, Object>> pods,
                                    List<Map<String, Object>> events) {
        if (crashLoopDetected(pods)) return DeploymentStatus.FAILED;
        if (crStatus == null || crStatus.isEmpty()) return DeploymentStatus.UNKNOWN;

        String state = asString(crStatus.get("state"));
        if (state == null) return DeploymentStatus.UNKNOWN;
        return switch (state.toLowerCase()) {
            case "ready" -> allReplsetsReady(crStatus)
                    ? DeploymentStatus.READY
                    : DeploymentStatus.APPLYING;
            case "error" -> DeploymentStatus.FAILED;
            case "initializing", "paused", "" -> DeploymentStatus.APPLYING;
            default -> DeploymentStatus.UNKNOWN;
        };
    }

    static boolean allReplsetsReady(Map<String, Object> crStatus) {
        Object rsRaw = crStatus.get("replsets");
        if (!(rsRaw instanceof Map<?, ?> rsMap) || rsMap.isEmpty()) return false;
        for (Object val : rsMap.values()) {
            if (!(val instanceof Map<?, ?> rs)) return false;
            String status = asString(rs.get("status"));
            if (!"ready".equalsIgnoreCase(status)) return false;
        }
        // Sharded deployments also need mongos ready.
        Object mongosRaw = crStatus.get("mongos");
        if (mongosRaw instanceof Map<?, ?> mongos) {
            String status = asString(mongos.get("status"));
            return "ready".equalsIgnoreCase(status);
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
