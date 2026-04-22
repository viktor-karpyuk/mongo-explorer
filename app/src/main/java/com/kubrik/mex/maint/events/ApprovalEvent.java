package com.kubrik.mex.maint.events;

import com.kubrik.mex.maint.model.Approval;

/**
 * v2.7 — Event-bus payload for approval state changes. Drives the
 * approval-queue chip in the toolbar (technical-spec §17).
 */
public record ApprovalEvent(
        String connectionId,
        String actionUuid,
        Approval.Status status,
        String approver,
        long at
) {}
