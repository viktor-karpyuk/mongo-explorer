package com.kubrik.mex.security.cis;

import com.kubrik.mex.security.access.AuthenticationRestriction;
import com.kubrik.mex.security.access.RoleBinding;
import com.kubrik.mex.security.access.UserRecord;
import com.kubrik.mex.security.access.UsersRolesFetcher;
import com.kubrik.mex.security.authn.AuthBackend;
import com.kubrik.mex.security.authn.AuthBackendProbe;
import com.kubrik.mex.security.cert.CertRecord;
import com.kubrik.mex.security.cis.rules.CertsNotImminent;
import com.kubrik.mex.security.cis.rules.CisRulePack;
import com.kubrik.mex.security.cis.rules.DisallowScramSha1;
import com.kubrik.mex.security.cis.rules.NoRootWithoutAuthRestrictions;
import com.kubrik.mex.security.cis.rules.RequireEncryptionAtRest;
import com.kubrik.mex.security.cis.rules.RequireScramSha256;
import com.kubrik.mex.security.encryption.EncryptionStatus;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-H2 — exercises every starter-pack rule through the runner.
 * Each test builds a minimal ComplianceContext that isolates the rule
 * under test, then asserts the verdict + the detail message operators
 * will read.
 */
class CisRulePackTest {

    @Test
    void RequireScramSha256_passes_when_256_is_enabled() {
        ComplianceContext ctx = ctxWithAuth(Map.of(
                AuthBackend.Mechanism.SCRAM_SHA_256, true));
        assertEquals(CisRule.Evaluation.Verdict.PASS,
                new RequireScramSha256().evaluate(ctx).verdict());
    }

    @Test
    void RequireScramSha256_fails_when_only_SHA1_is_enabled() {
        ComplianceContext ctx = ctxWithAuth(Map.of(
                AuthBackend.Mechanism.SCRAM_SHA_1, true));
        CisRule.Evaluation e = new RequireScramSha256().evaluate(ctx);
        assertEquals(CisRule.Evaluation.Verdict.FAIL, e.verdict());
        assertTrue(e.detail().toLowerCase().contains("scram-sha-256"));
    }

    @Test
    void DisallowScramSha1_fails_when_SHA1_is_advertised() {
        ComplianceContext ctx = ctxWithAuth(Map.of(
                AuthBackend.Mechanism.SCRAM_SHA_256, true,
                AuthBackend.Mechanism.SCRAM_SHA_1, true));
        assertEquals(CisRule.Evaluation.Verdict.FAIL,
                new DisallowScramSha1().evaluate(ctx).verdict());
    }

    @Test
    void DisallowScramSha1_passes_when_only_SHA256_is_enabled() {
        ComplianceContext ctx = ctxWithAuth(Map.of(
                AuthBackend.Mechanism.SCRAM_SHA_256, true));
        assertEquals(CisRule.Evaluation.Verdict.PASS,
                new DisallowScramSha1().evaluate(ctx).verdict());
    }

    @Test
    void RequireEncryptionAtRest_fails_when_any_node_is_unencrypted() {
        ComplianceContext ctx = ctxWithEncryption(
                enc("h1:27017", true, EncryptionStatus.Keystore.KMIP),
                enc("h2:27017", false, EncryptionStatus.Keystore.NONE));
        CisRule.Evaluation e = new RequireEncryptionAtRest().evaluate(ctx);
        assertEquals(CisRule.Evaluation.Verdict.FAIL, e.verdict());
        assertTrue(e.detail().contains("h2:27017"));
    }

    @Test
    void RequireEncryptionAtRest_passes_when_every_node_encrypted() {
        ComplianceContext ctx = ctxWithEncryption(
                enc("h1:27017", true, EncryptionStatus.Keystore.KMIP),
                enc("h2:27017", true, EncryptionStatus.Keystore.KMIP));
        assertEquals(CisRule.Evaluation.Verdict.PASS,
                new RequireEncryptionAtRest().evaluate(ctx).verdict());
    }

    @Test
    void RequireEncryptionAtRest_NotApplicable_when_no_probe_data() {
        ComplianceContext ctx = emptyCtx();
        assertEquals(CisRule.Evaluation.Verdict.NOT_APPLICABLE,
                new RequireEncryptionAtRest().evaluate(ctx).verdict());
    }

    @Test
    void CertsNotImminent_fails_on_amber_certs() {
        long nowMs = Instant.parse("2026-04-21T00:00:00Z").toEpochMilli();
        CertRecord amber = new CertRecord("h1:27017", "CN=h1", "CN=ca",
                List.of(), nowMs, nowMs + 10L * 86_400_000L, "aa",
                "0".repeat(64));
        ComplianceContext ctx = ctxWithCerts(List.of(amber));

        CisRule.Evaluation e = new CertsNotImminent(
                Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.UTC)
        ).evaluate(ctx);
        assertEquals(CisRule.Evaluation.Verdict.FAIL, e.verdict());
        assertTrue(e.detail().contains("h1:27017"));
    }

    @Test
    void CertsNotImminent_passes_on_long_lived_certs() {
        long nowMs = Instant.parse("2026-04-21T00:00:00Z").toEpochMilli();
        CertRecord ok = new CertRecord("h1:27017", "CN=h1", "CN=ca",
                List.of(), nowMs, nowMs + 365L * 86_400_000L, "aa",
                "0".repeat(64));
        ComplianceContext ctx = ctxWithCerts(List.of(ok));
        assertEquals(CisRule.Evaluation.Verdict.PASS,
                new CertsNotImminent(
                        Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.UTC))
                        .evaluate(ctx).verdict());
    }

    @Test
    void NoRootWithoutAuthRestrictions_fails_on_unguarded_root() {
        UserRecord dba = new UserRecord("admin", "dba",
                List.of(new RoleBinding("root", "admin")),
                List.of(), List.of());
        ComplianceContext ctx = ctxWithUsers(List.of(dba));
        CisRule.Evaluation e = new NoRootWithoutAuthRestrictions().evaluate(ctx);
        assertEquals(CisRule.Evaluation.Verdict.FAIL, e.verdict());
        assertTrue(e.detail().contains("dba@admin"));
    }

    @Test
    void NoRootWithoutAuthRestrictions_passes_with_IP_allowlist() {
        UserRecord dba = new UserRecord("admin", "dba",
                List.of(new RoleBinding("root", "admin")),
                List.of(),
                List.of(new AuthenticationRestriction(
                        List.of("10.0.0.0/24"), List.of())));
        ComplianceContext ctx = ctxWithUsers(List.of(dba));
        assertEquals(CisRule.Evaluation.Verdict.PASS,
                new NoRootWithoutAuthRestrictions().evaluate(ctx).verdict());
    }

    @Test
    void rulePack_exposes_all_five_starter_rules() {
        assertEquals(5, CisRulePack.all().size());
        // IDs follow the CIS benchmark numbering so reports are
        // searchable against printed audit guides.
        List<String> ids = CisRulePack.all().stream().map(CisRule::id).toList();
        assertTrue(ids.contains("CIS-6.1.3"));
        assertTrue(ids.contains("CIS-6.1.4"));
        assertTrue(ids.contains("CIS-2.1"));
        assertTrue(ids.contains("CIS-2.5"));
        assertTrue(ids.contains("CIS-5.2"));
    }

    /* ============================== fixtures ============================== */

    private static ComplianceContext emptyCtx() {
        return new ComplianceContext("cx-a",
                new UsersRolesFetcher.Snapshot(List.of(), List.of()),
                new AuthBackendProbe.Snapshot(List.of(), 0L),
                List.of(), List.of());
    }

    private static ComplianceContext ctxWithAuth(Map<AuthBackend.Mechanism, Boolean> mechs) {
        List<AuthBackend> backends = new java.util.ArrayList<>();
        for (AuthBackend.Mechanism m : AuthBackend.Mechanism.values()) {
            backends.add(new AuthBackend(m,
                    Boolean.TRUE.equals(mechs.get(m)), Map.of()));
        }
        return new ComplianceContext("cx-a",
                new UsersRolesFetcher.Snapshot(List.of(), List.of()),
                new AuthBackendProbe.Snapshot(backends, 0L),
                List.of(), List.of());
    }

    private static ComplianceContext ctxWithEncryption(EncryptionStatus... statuses) {
        return new ComplianceContext("cx-a",
                new UsersRolesFetcher.Snapshot(List.of(), List.of()),
                new AuthBackendProbe.Snapshot(List.of(), 0L),
                List.of(statuses), List.of());
    }

    private static ComplianceContext ctxWithCerts(List<CertRecord> certs) {
        return new ComplianceContext("cx-a",
                new UsersRolesFetcher.Snapshot(List.of(), List.of()),
                new AuthBackendProbe.Snapshot(List.of(), 0L),
                List.of(), certs);
    }

    private static ComplianceContext ctxWithUsers(List<UserRecord> users) {
        return new ComplianceContext("cx-a",
                new UsersRolesFetcher.Snapshot(users, List.of()),
                new AuthBackendProbe.Snapshot(List.of(), 0L),
                List.of(), List.of());
    }

    private static EncryptionStatus enc(String host, boolean enabled,
                                          EncryptionStatus.Keystore ks) {
        return new EncryptionStatus(host, enabled, "wiredTiger", ks, null,
                "AES256-GCM", "serverStatus");
    }
}
