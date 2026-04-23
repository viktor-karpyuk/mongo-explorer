package com.kubrik.mex.maint.approval;

import com.kubrik.mex.maint.model.Approval;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * v2.7 Q2.7-A — Orchestrates the two-person-approval gate every
 * destructive maintenance action passes through.
 *
 * <h2>Three modes</h2>
 * <ul>
 *   <li><b>SOLO</b> — explicit per-connection opt-in for dev clusters.
 *       Request transitions directly to APPROVED in the same call.</li>
 *   <li><b>TWO_PERSON</b> — default. Request lands PENDING; a reviewer
 *       calls {@link #approveTwoPerson} with their name and a reason.
 *       The signature covers the payload hash so a post-approval
 *       tamper is detectable.</li>
 *   <li><b>TOKEN</b> — reviewer runs {@link #signDescriptor} in their
 *       own install (or the same install but an isolated session),
 *       hands the compact JWS to the executor, and
 *       {@link #requestToken} stores it as APPROVED in one shot.</li>
 * </ul>
 *
 * <p>Approvals are single-use. {@link #markConsumed} is how the
 * maintenance runner tells the service the action it gated has fired;
 * the transition from APPROVED to CONSUMED is idempotent but cannot
 * be reversed.</p>
 *
 * <p>Expiration is a separate concern from consumption: a PENDING
 * approval beyond its {@code expires_at} becomes EXPIRED via
 * {@link #sweepExpired}; this runs from the scheduler tick and from
 * pane refresh so a long-idle approval queue stays trim.</p>
 */
public final class ApprovalService {

    /** Default approval window — 1 h. Teams with geo-split approvers
     *  override per connection via a future setting; milestone §9.1
     *  tracks the open question. */
    public static final long DEFAULT_EXPIRY_MS = 60L * 60L * 1000L;

    private final ApprovalDao dao;
    private final JwsSigner jws;
    private final java.util.function.LongSupplier clock;

    public ApprovalService(ApprovalDao dao, JwsSigner jws) {
        this(dao, jws, System::currentTimeMillis);
    }

    /** Test-visible clock seam; production callers use the no-clock
     *  ctor so every call-site reads the same system clock. */
    ApprovalService(ApprovalDao dao, JwsSigner jws,
                    java.util.function.LongSupplier clock) {
        this.dao = dao;
        this.jws = jws;
        this.clock = clock;
    }

    /* =========================== request paths =========================== */

    /** SOLO: the request is approved in one shot. The caller must
     *  already hold the per-connection "solo mode" opt-in — that
     *  policy decision lives with the UI, not here. */
    public Approval.Row requestSolo(Approval.Request r) {
        long now = clock.getAsLong();
        String sig = signApprovalDescriptor(r, r.requestedBy(), now);
        Approval.Row row = new Approval.Row(
                -1, r.actionUuid(), r.connectionId(), r.actionName(),
                r.payloadJson(), r.payloadHash(), r.requestedAt(),
                r.requestedBy(), Approval.Mode.SOLO,
                r.requestedBy(), now, sig,
                Approval.Status.APPROVED,
                now + DEFAULT_EXPIRY_MS,
                /*reviewerJws=*/null);
        return dao.insert(row);
    }

    /** TWO_PERSON: PENDING on insert. Reviewer then calls
     *  {@link #approveTwoPerson} to flip it to APPROVED. */
    public Approval.Row requestTwoPerson(Approval.Request r) {
        long now = clock.getAsLong();
        Approval.Row row = new Approval.Row(
                -1, r.actionUuid(), r.connectionId(), r.actionName(),
                r.payloadJson(), r.payloadHash(), r.requestedAt(),
                r.requestedBy(), Approval.Mode.TWO_PERSON,
                null, null, null,
                Approval.Status.PENDING,
                now + DEFAULT_EXPIRY_MS,
                /*reviewerJws=*/null);
        return dao.insert(row);
    }

    /** TOKEN: the executor receives a pre-signed descriptor from the
     *  reviewer. Stores the approval directly as APPROVED if the JWS
     *  validates against this install's evidence key and the payload
     *  hash inside the token matches what the executor sees. */
    public Optional<Approval.Row> requestToken(Approval.Request r, String reviewerJws) {
        Optional<Map<String, Object>> claims = jws.verify(reviewerJws);
        if (claims.isEmpty()) return Optional.empty();
        Map<String, Object> c = claims.get();
        // Token must match the action the executor is about to run —
        // replay + swap attacks are caught here.
        if (!r.actionUuid().equals(c.get("auuid"))) return Optional.empty();
        if (!r.actionName().equals(c.get("a"))) return Optional.empty();
        if (!r.payloadHash().equals(c.get("ph"))) return Optional.empty();

        // exp is REQUIRED and MUST be a Number. A missing or
        // mistyped exp claim was previously treated as "no expiry
        // check" which silently gave the executor a fresh 1-hour
        // window — a replay-window bypass. Now it's a hard reject.
        Object exp = c.get("exp");
        if (!(exp instanceof Number)) return Optional.empty();
        long expMs = ((Number) exp).longValue();
        long now = clock.getAsLong();
        if (now > expMs) return Optional.empty();

        Object reviewer = c.get("rev");
        String reviewerName = reviewer == null ? "token" : reviewer.toString();

        // Re-sign the approval descriptor locally so approval_sig
        // column is type-uniform across modes (SOLO / TWO_PERSON also
        // store a descriptor signature, not the raw request token).
        // The reviewer's original JWS is ALSO persisted — v2.7 review
        // GA — so an auditor can later verify chain-of-trust against
        // the reviewer's install key.
        String descriptorSig = signApprovalDescriptor(r, reviewerName, now);

        Approval.Row row = new Approval.Row(
                -1, r.actionUuid(), r.connectionId(), r.actionName(),
                r.payloadJson(), r.payloadHash(), r.requestedAt(),
                r.requestedBy(), Approval.Mode.TOKEN,
                reviewerName, now, descriptorSig,
                Approval.Status.APPROVED, expMs, reviewerJws);
        return Optional.of(dao.insert(row));
    }

    /* =========================== reviewer paths =========================== */

    /** In-tool reviewer action: flip a PENDING TWO_PERSON row to
     *  APPROVED with the reviewer name + approval signature. Returns
     *  {@code true} iff the row was PENDING. */
    public boolean approveTwoPerson(String actionUuid, String reviewerName) {
        Optional<Approval.Row> row = dao.byActionUuid(actionUuid);
        if (row.isEmpty()) return false;
        Approval.Row r = row.get();
        if (r.status() != Approval.Status.PENDING) return false;
        if (r.requestedBy().equals(reviewerName)) {
            // Self-approval defeats the whole point of TWO_PERSON.
            return false;
        }
        long now = clock.getAsLong();
        String sig = signApprovalDescriptor(
                new Approval.Request(r.actionUuid(), r.connectionId(),
                        r.actionName(), r.payloadJson(), r.payloadHash(),
                        r.requestedBy(), r.requestedAt(), r.mode()),
                reviewerName, now);
        return dao.approve(actionUuid, reviewerName, sig, now);
    }

    /** Reviewer signs a descriptor so the executor's install can
     *  consume it as a TOKEN approval. Uses {@link JwsSigner}'s HS256
     *  with the v2.6 evidence key — cross-install requires prior
     *  out-of-band key import (NG-2.7-4). */
    public String signDescriptor(Approval.Request r, String reviewerName, long expiresAt) {
        Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("auuid", r.actionUuid());
        claims.put("a", r.actionName());
        claims.put("ph", r.payloadHash());
        claims.put("rev", reviewerName);
        claims.put("iat", clock.getAsLong());
        claims.put("exp", expiresAt);
        return jws.sign(claims);
    }

    /** Convenience — sign with the default 1-hour expiry. */
    public String signDescriptor(Approval.Request r, String reviewerName) {
        return signDescriptor(r, reviewerName,
                clock.getAsLong() + DEFAULT_EXPIRY_MS);
    }

    /* ============================ lifecycle ============================ */

    public boolean reject(String actionUuid) { return dao.reject(actionUuid); }

    public boolean markConsumed(String actionUuid) {
        return dao.markConsumed(actionUuid);
    }

    /** Latest status for an action. EMPTY if the approval row was
     *  never persisted (unknown uuid). */
    public Optional<Approval.Status> status(String actionUuid) {
        return dao.byActionUuid(actionUuid).map(Approval.Row::status);
    }

    public int sweepExpired() { return dao.expireOverdue(clock.getAsLong()); }

    /** Convenience for UI panes: sweep expired rows then return the
     *  remaining PENDING queue for a connection, in one call. */
    public java.util.List<Approval.Row> sweepAllAndReload(String connectionId) {
        sweepExpired();
        return dao.listPending(connectionId);
    }

    /** Non-sweeping list of PENDING rows for a connection. Panes that
     *  throttle their sweep cadence (approvals queue refresh) call
     *  this directly instead of sweepAllAndReload. */
    public java.util.List<Approval.Row> listPending(String connectionId) {
        return dao.listPending(connectionId);
    }

    /* ============================ internals ============================ */

    /** Signs the approval descriptor (action-uuid + payload hash +
     *  reviewer + timestamp) so the resulting {@code approval_sig}
     *  field is verifiable later even without replaying the request
     *  flow. */
    private String signApprovalDescriptor(Approval.Request r, String approver, long at) {
        Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("auuid", r.actionUuid());
        claims.put("ph", r.payloadHash());
        claims.put("rev", approver);
        claims.put("at", at);
        return jws.sign(claims);
    }
}
