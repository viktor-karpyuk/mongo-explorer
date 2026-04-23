package com.kubrik.mex.k8s.model;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 — Result of a single liveness + version probe against a
 * {@link K8sClusterRef}.
 *
 * <p>Shape mirrors the {@code REACHABLE / UNREACHABLE / AUTH_FAILED /
 * TIMED_OUT} states the Clusters pane renders. The service version
 * + node count fields are best-effort: a probe that is reachable but
 * lacks the RBAC to list nodes reports {@code reachable=true},
 * {@code nodeCount=empty}, and no error.</p>
 */
public record ClusterProbeResult(
        Status status,
        Optional<String> serverVersion,
        Optional<Integer> nodeCount,
        Optional<String> errorMessage,
        long probedAt) {

    public enum Status {
        REACHABLE,
        UNREACHABLE,       // network / DNS / TLS error
        AUTH_FAILED,       // kubeconfig loaded but 401 / 403 on /version
        PLUGIN_MISSING,    // exec-plugin binary not on PATH
        TIMED_OUT          // server never answered within the probe budget
    }

    public ClusterProbeResult {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(serverVersion, "serverVersion");
        Objects.requireNonNull(nodeCount, "nodeCount");
        Objects.requireNonNull(errorMessage, "errorMessage");
    }

    public boolean ok() {
        return status == Status.REACHABLE;
    }
}
