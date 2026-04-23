package com.kubrik.mex.maint.approval;

import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.maint.model.Approval;
import com.kubrik.mex.security.EvidenceSigner;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 Q2.7-A — Covers each {@link ApprovalService} path: SOLO,
 * TWO_PERSON success + self-approval rejection, TOKEN roundtrip +
 * tamper + replay, expiry sweep, and the consumption transition.
 */
class ApprovalServiceTest {

    @TempDir Path dataDir;

    private Database db;
    private ApprovalService svc;
    private AtomicLong clock;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        ApprovalDao dao = new ApprovalDao(db);
        JwsSigner jws = new JwsSigner(new EvidenceSigner(db, new Crypto()));
        clock = new AtomicLong(1_700_000_000_000L);
        svc = new ApprovalService(dao, jws, clock::get);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    /* =============================== SOLO =============================== */

    @Test
    void solo_request_is_approved_in_one_shot() {
        Approval.Request r = sample("rcfg.apply", "alice");
        Approval.Row row = svc.requestSolo(r);

        assertEquals(Approval.Status.APPROVED, row.status());
        assertEquals(Approval.Mode.SOLO, row.mode());
        assertEquals("alice", row.approver());
        assertNotNull(row.approvalSig());
        assertEquals(Optional.of(Approval.Status.APPROVED),
                svc.status(r.actionUuid()));
    }

    /* ============================ TWO_PERSON ============================ */

    @Test
    void two_person_request_lands_pending_then_approves() {
        Approval.Request r = sample("rcfg.apply", "alice");
        Approval.Row pending = svc.requestTwoPerson(r);
        assertEquals(Approval.Status.PENDING, pending.status());

        assertTrue(svc.approveTwoPerson(r.actionUuid(), "bob"));
        assertEquals(Optional.of(Approval.Status.APPROVED),
                svc.status(r.actionUuid()));
    }

    @Test
    void self_approval_is_refused() {
        Approval.Request r = sample("rcfg.apply", "alice");
        svc.requestTwoPerson(r);
        // The point of TWO_PERSON is a second human; the requester
        // can't also be the reviewer.
        assertFalse(svc.approveTwoPerson(r.actionUuid(), "alice"));
        assertEquals(Optional.of(Approval.Status.PENDING),
                svc.status(r.actionUuid()));
    }

    @Test
    void approving_unknown_action_is_noop() {
        assertFalse(svc.approveTwoPerson("no-such-uuid", "bob"));
    }

    @Test
    void approving_already_approved_is_noop() {
        Approval.Request r = sample("rcfg.apply", "alice");
        svc.requestTwoPerson(r);
        assertTrue(svc.approveTwoPerson(r.actionUuid(), "bob"));
        // A second approval attempt must not silently overwrite the
        // first — prevents a stale UI from double-counting.
        assertFalse(svc.approveTwoPerson(r.actionUuid(), "carol"));
    }

    /* ============================== TOKEN ============================== */

    @Test
    void token_roundtrip_approves_the_action() {
        Approval.Request r = sample("rcfg.apply", "alice");
        String token = svc.signDescriptor(r, "bob");
        Optional<Approval.Row> row = svc.requestToken(r, token);
        assertTrue(row.isPresent());
        assertEquals(Approval.Status.APPROVED, row.get().status());
        assertEquals("bob", row.get().approver());
    }

    @Test
    void tampered_token_signature_fails_verify() {
        Approval.Request r = sample("rcfg.apply", "alice");
        String token = svc.signDescriptor(r, "bob");
        // Flip one character in the signature segment — MessageDigest
        // compare must reject.
        String tampered = token.substring(0, token.length() - 2)
                + (token.charAt(token.length() - 2) == 'a' ? "bb" : "aa");
        assertTrue(svc.requestToken(r, tampered).isEmpty());
    }

    @Test
    void token_for_different_payload_is_refused() {
        Approval.Request reviewed = sample("rcfg.apply", "alice");
        String token = svc.signDescriptor(reviewed, "bob");

        // Executor asks to use the token for a DIFFERENT payload hash —
        // the replay-and-swap attack. Must be refused.
        Approval.Request swapped = new Approval.Request(
                reviewed.actionUuid(), reviewed.connectionId(),
                reviewed.actionName(), reviewed.payloadJson(),
                "SHA256-of-something-else",
                reviewed.requestedBy(), reviewed.requestedAt(),
                reviewed.mode());
        assertTrue(svc.requestToken(swapped, token).isEmpty());
    }

    @Test
    void token_for_different_action_name_is_refused() {
        Approval.Request reviewed = sample("rcfg.apply", "alice");
        String token = svc.signDescriptor(reviewed, "bob");

        // Reviewer signed for rs.reconfig; executor tries to use the
        // same token to apply a parameter change.
        Approval.Request swapped = new Approval.Request(
                reviewed.actionUuid(), reviewed.connectionId(),
                "param.set", reviewed.payloadJson(),
                reviewed.payloadHash(), reviewed.requestedBy(),
                reviewed.requestedAt(), reviewed.mode());
        assertTrue(svc.requestToken(swapped, token).isEmpty());
    }

    @Test
    void expired_token_is_refused() {
        Approval.Request r = sample("rcfg.apply", "alice");
        String token = svc.signDescriptor(r, "bob",
                clock.get() + 1000);  // expires in 1 s
        clock.addAndGet(2000);           // now 1 s past expiry
        assertTrue(svc.requestToken(r, token).isEmpty());
    }

    @Test
    void token_without_exp_claim_is_refused() {
        // Review round 2 regression: a crafted token lacking `exp`
        // was silently treated as "no expiry" and given a fresh
        // 1-hour window. Must now hard-reject.
        Approval.Request r = sample("rcfg.apply", "alice");
        // Build a JWS with no exp claim, signed with this install's
        // key. The sig will verify; the service must still refuse.
        com.kubrik.mex.maint.approval.JwsSigner signer =
                new com.kubrik.mex.maint.approval.JwsSigner(
                        new com.kubrik.mex.security.EvidenceSigner(
                                db, new com.kubrik.mex.core.Crypto()));
        String tokenNoExp = signer.sign(java.util.Map.of(
                "auuid", r.actionUuid(),
                "a", r.actionName(),
                "ph", r.payloadHash(),
                "rev", "bob",
                "iat", clock.get()));
        assertTrue(svc.requestToken(r, tokenNoExp).isEmpty(),
                "token missing exp claim must be refused");
    }

    @Test
    void approve_of_expired_pending_row_is_refused() {
        // Review round 2 regression: sweep-then-approve race could
        // flip an EXPIRED-eligible row to APPROVED if the sweep
        // hadn't fired yet. approveTwoPerson now rechecks expires_at
        // atomically in the UPDATE.
        Approval.Request r = sample("rcfg.apply", "alice");
        svc.requestTwoPerson(r);
        clock.addAndGet(ApprovalService.DEFAULT_EXPIRY_MS + 1);
        // Intentionally DO NOT sweep — simulate the race window.
        assertFalse(svc.approveTwoPerson(r.actionUuid(), "bob"),
                "approve must recheck expiry atomically");
    }

    /* ============================ lifecycle ============================ */

    @Test
    void consumed_transition_is_one_way() {
        Approval.Request r = sample("rcfg.apply", "alice");
        svc.requestSolo(r);
        assertTrue(svc.markConsumed(r.actionUuid()));
        // Second markConsumed has no APPROVED row to flip — idempotent
        // at the SQL level, no hard failure.
        assertFalse(svc.markConsumed(r.actionUuid()));
        assertEquals(Optional.of(Approval.Status.CONSUMED),
                svc.status(r.actionUuid()));
    }

    @Test
    void expiry_sweep_flips_overdue_pending_rows() {
        Approval.Request r = sample("rcfg.apply", "alice");
        svc.requestTwoPerson(r);

        // Advance the clock past the default 1 h expiry.
        clock.addAndGet(ApprovalService.DEFAULT_EXPIRY_MS + 1);
        int swept = svc.sweepExpired();
        assertEquals(1, swept);
        assertEquals(Optional.of(Approval.Status.EXPIRED),
                svc.status(r.actionUuid()));
    }

    @Test
    void reject_transitions_pending_to_rejected() {
        Approval.Request r = sample("rcfg.apply", "alice");
        svc.requestTwoPerson(r);
        assertTrue(svc.reject(r.actionUuid()));
        assertEquals(Optional.of(Approval.Status.REJECTED),
                svc.status(r.actionUuid()));
    }

    /* ============================ fixtures ============================ */

    private Approval.Request sample(String actionName, String requestedBy) {
        return Approval.Request.create("cx-1", actionName,
                "{\"spec\":\"redacted\"}", "SHA256-abc", requestedBy,
                Approval.Mode.TWO_PERSON);
    }
}
