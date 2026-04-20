package com.kubrik.mex.cluster.safety;

import java.util.List;
import java.util.Map;

/**
 * v2.4 SAFE-OPS-1 — destructive-command descriptor. Used by the
 * {@link com.kubrik.mex.cluster.dryrun.DryRunRenderer} to emit a preview and
 * by the dispatcher to execute. {@code args} is an order-preserving map so
 * shell-equivalent rendering is deterministic.
 */
public sealed interface Command {

    /** Canonical machine identifier, e.g. {@code replSetStepDown}. */
    String name();

    /** {@code ui_source} string persisted in {@code ops_audit}. */
    String uiSource();

    /** The target identity the typed-confirm dialog must match against (host:port / opid / object id). */
    String typedConfirmTarget();

    record StepDown(String host, int stepDownSecs, int catchUpSecs) implements Command {
        public StepDown {
            if (host == null || host.isBlank()) throw new IllegalArgumentException("host");
            if (stepDownSecs < 1 || stepDownSecs > 86_400) throw new IllegalArgumentException("stepDownSecs");
            if (catchUpSecs < 0 || catchUpSecs > stepDownSecs) throw new IllegalArgumentException("catchUpSecs");
        }
        @Override public String name() { return "replSetStepDown"; }
        @Override public String uiSource() { return "cluster.topology"; }
        @Override public String typedConfirmTarget() { return host; }
    }

    record Freeze(String host, int seconds) implements Command {
        public Freeze {
            if (host == null || host.isBlank()) throw new IllegalArgumentException("host");
            if (seconds < 0 || seconds > 3_600) throw new IllegalArgumentException("seconds");
        }
        @Override public String name() { return "replSetFreeze"; }
        @Override public String uiSource() { return "cluster.topology"; }
        @Override public String typedConfirmTarget() { return host; }
    }

    record KillOp(String host, long opid) implements Command {
        public KillOp {
            if (host == null || host.isBlank()) throw new IllegalArgumentException("host");
            if (opid < 0) throw new IllegalArgumentException("opid");
        }
        @Override public String name() { return "killOp"; }
        @Override public String uiSource() { return "cluster.ops"; }
        @Override public String typedConfirmTarget() { return Long.toString(opid); }
    }

    record MoveChunk(String ns, Map<String, Object> boundsMin, Map<String, Object> boundsMax,
                     String toShard, boolean waitForDelete, String writeConcern) implements Command {
        public MoveChunk {
            if (ns == null || !ns.contains(".")) throw new IllegalArgumentException("ns");
            if (toShard == null || toShard.isBlank()) throw new IllegalArgumentException("toShard");
            if (writeConcern == null || writeConcern.isBlank()) writeConcern = "majority";
            boundsMin = boundsMin == null ? Map.of() : Map.copyOf(boundsMin);
            boundsMax = boundsMax == null ? Map.of() : Map.copyOf(boundsMax);
        }
        @Override public String name() { return "moveChunk"; }
        @Override public String uiSource() { return "cluster.balancer"; }
        @Override public String typedConfirmTarget() { return toShard; }
    }

    record BalancerStart(String clusterName) implements Command {
        public BalancerStart {
            if (clusterName == null || clusterName.isBlank()) throw new IllegalArgumentException("clusterName");
        }
        @Override public String name() { return "sh.startBalancer"; }
        @Override public String uiSource() { return "cluster.balancer"; }
        @Override public String typedConfirmTarget() { return clusterName; }
    }

    record BalancerStop(String clusterName) implements Command {
        public BalancerStop {
            if (clusterName == null || clusterName.isBlank()) throw new IllegalArgumentException("clusterName");
        }
        @Override public String name() { return "sh.stopBalancer"; }
        @Override public String uiSource() { return "cluster.balancer"; }
        @Override public String typedConfirmTarget() { return clusterName; }
    }

    record BalancerWindow(String clusterName, String startHhmm, String stopHhmm) implements Command {
        public BalancerWindow {
            if (clusterName == null || clusterName.isBlank()) throw new IllegalArgumentException("clusterName");
            if (!isHhmm(startHhmm) || !isHhmm(stopHhmm)) throw new IllegalArgumentException("HH:MM required");
        }
        @Override public String name() { return "balancerWindow"; }
        @Override public String uiSource() { return "cluster.balancer"; }
        @Override public String typedConfirmTarget() { return clusterName; }
        private static boolean isHhmm(String s) {
            return s != null && s.matches("^([01]\\d|2[0-3]):[0-5]\\d$");
        }
    }

    record AddTagRange(String ns, Map<String, Object> min, Map<String, Object> max, String zone) implements Command {
        public AddTagRange {
            if (ns == null || !ns.contains(".")) throw new IllegalArgumentException("ns");
            if (zone == null || zone.isBlank()) throw new IllegalArgumentException("zone");
            min = min == null ? Map.of() : Map.copyOf(min);
            max = max == null ? Map.of() : Map.copyOf(max);
        }
        @Override public String name() { return "sh.addTagRange"; }
        @Override public String uiSource() { return "cluster.balancer"; }
        @Override public String typedConfirmTarget() { return zone; }
    }

    record RemoveTagRange(String ns, Map<String, Object> min, Map<String, Object> max) implements Command {
        public RemoveTagRange {
            if (ns == null || !ns.contains(".")) throw new IllegalArgumentException("ns");
            min = min == null ? Map.of() : Map.copyOf(min);
            max = max == null ? Map.of() : Map.copyOf(max);
        }
        @Override public String name() { return "sh.removeTagRange"; }
        @Override public String uiSource() { return "cluster.balancer"; }
        @Override public String typedConfirmTarget() { return ns; }
    }

    /** Used for the kill-switch toggle audit row — not a mongo command. */
    record KillSwitchToggle(boolean engaging) implements Command {
        @Override public String name() { return "ui.kill_switch"; }
        @Override public String uiSource() { return "header"; }
        @Override public String typedConfirmTarget() { return engaging ? "engage" : "disengage"; }
    }

    /** Useful when a consumer wants the canonical role list the command implies (e.g., for role-probe gate). */
    default List<String> requiredRoles() {
        return switch (this) {
            case StepDown s -> List.of("clusterManager", "root");
            case Freeze f -> List.of("clusterManager", "root");
            case KillOp k -> List.of("killAnyCursor", "root");
            case MoveChunk m -> List.of("clusterManager", "root");
            case BalancerStart b -> List.of("clusterManager", "root");
            case BalancerStop b -> List.of("clusterManager", "root");
            case BalancerWindow b -> List.of("clusterManager", "root");
            case AddTagRange a -> List.of("clusterManager", "root");
            case RemoveTagRange r -> List.of("clusterManager", "root");
            case KillSwitchToggle k -> List.of();
        };
    }
}
