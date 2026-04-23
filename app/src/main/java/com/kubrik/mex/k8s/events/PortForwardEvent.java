package com.kubrik.mex.k8s.events;

/**
 * v2.8.1 Q2.8.1-C4 — Port-forward lifecycle feed.
 *
 * <p>Emitted at each transition: Opened → (Error*) → Closed. The
 * Clusters pane + status bar subscribe so the UI can show a live
 * "forwarding 27017 → pod" chip. The {@code auditRowId} is the
 * primary key of the matching {@code portforward_audit} row — a
 * stable identifier across an Open / Close pair.</p>
 */
public sealed interface PortForwardEvent {

    String connectionId();
    long auditRowId();

    record Opened(String connectionId, long auditRowId,
                   int localPort, long at) implements PortForwardEvent {}

    record Closed(String connectionId, long auditRowId,
                   String reason, long at) implements PortForwardEvent {}

    record Error(String connectionId, long auditRowId,
                  String message, long at) implements PortForwardEvent {}
}
