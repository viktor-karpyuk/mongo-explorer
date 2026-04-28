package com.kubrik.mex.k8s.model;

import java.util.Objects;

/**
 * v2.8.1 Q2.8.1-C1 — Metadata about a live port-forward.
 *
 * <p>Identity is the tuple {@code (clusterId, connectionId,
 * localPort)} — the same connection can legally hold multiple
 * forwards (e.g. a sharded cluster exposes both mongos and its
 * config replset in future flows), so {@code connectionId} alone is
 * not enough. {@code auditRowId} is the {@code portforward_audit.id}
 * for the open event; the close event updates that same row.</p>
 */
public record PortForwardSession(
        long clusterId,
        String connectionId,
        PortForwardTarget target,
        int localPort,
        long openedAt,
        long auditRowId) {

    public PortForwardSession {
        Objects.requireNonNull(connectionId, "connectionId");
        Objects.requireNonNull(target, "target");
    }
}
