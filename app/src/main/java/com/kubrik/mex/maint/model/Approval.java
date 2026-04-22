package com.kubrik.mex.maint.model;

import java.util.Objects;
import java.util.UUID;

/**
 * v2.7 Q2.7-A — Represents a two-person-approval checkpoint for a
 * destructive maintenance action. Every *Apply* wizard mints a
 * {@link Request} describing what's about to happen, hands it to the
 * approval service, and blocks until a same-install reviewer (TWO_PERSON),
 * a signed approval token (TOKEN), or an explicit per-connection opt-in
 * (SOLO) unlocks execution.
 *
 * <p>Instances are persisted into the {@code approvals} table; the
 * record mirrors that schema one-to-one so callers can freely round-trip
 * through the DAO. {@link Mode} governs which path the request takes;
 * {@link Status} tracks its lifecycle.</p>
 */
public final class Approval {

    /** Approval mode. Wizards read this from a per-connection setting
     *  and pre-populate the dialog; users can override per action. */
    public enum Mode { SOLO, TWO_PERSON, TOKEN }

    /** Approval lifecycle. {@link #CONSUMED} is set atomically with
     *  the audit row of the maintenance action it gated. */
    public enum Status { PENDING, APPROVED, REJECTED, EXPIRED, CONSUMED }

    /** An approval request — the input side of the service. {@code
     *  payloadJson} is the redacted spec (secrets already stripped);
     *  {@code payloadHash} is a SHA-256 of the canonical spec and is
     *  what the approval signature covers so a post-approval payload
     *  tamper is detectable. */
    public record Request(
            String actionUuid,
            String connectionId,
            String actionName,
            String payloadJson,
            String payloadHash,
            String requestedBy,
            long requestedAt,
            Mode mode
    ) {
        public Request {
            Objects.requireNonNull(actionUuid, "actionUuid");
            Objects.requireNonNull(connectionId, "connectionId");
            Objects.requireNonNull(actionName, "actionName");
            Objects.requireNonNull(payloadJson, "payloadJson");
            Objects.requireNonNull(payloadHash, "payloadHash");
            Objects.requireNonNull(requestedBy, "requestedBy");
            Objects.requireNonNull(mode, "mode");
        }

        /** Convenience for callers that don't pre-compute the uuid. */
        public static Request create(String connectionId, String actionName,
                                     String payloadJson, String payloadHash,
                                     String requestedBy, Mode mode) {
            return new Request(UUID.randomUUID().toString(), connectionId,
                    actionName, payloadJson, payloadHash, requestedBy,
                    System.currentTimeMillis(), mode);
        }
    }

    /** A persisted approval row — the output side. {@code approvalSig}
     *  is a JWS over the payload hash + action-uuid; verifying it is
     *  how the executor proves the approval is genuine. */
    public record Row(
            long id,
            String actionUuid,
            String connectionId,
            String actionName,
            String payloadJson,
            String payloadHash,
            long requestedAt,
            String requestedBy,
            Mode mode,
            String approver,
            Long approvedAt,
            String approvalSig,
            Status status,
            Long expiresAt
    ) {}

    private Approval() {}
}
