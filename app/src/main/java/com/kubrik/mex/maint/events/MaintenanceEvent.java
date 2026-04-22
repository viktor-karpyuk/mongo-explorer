package com.kubrik.mex.maint.events;

/**
 * v2.7 — Event-bus payload for maintenance-wizard lifecycle. One
 * per wizard run; the UI subscribes to surface toasts / history.
 */
public record MaintenanceEvent(
        String connectionId,
        String actionName,
        String actionUuid,
        Stage stage,
        String message,
        long at
) {
    public enum Stage { STARTED, APPROVED, RUNNING, SUCCEEDED, FAILED, ROLLED_BACK }
}
