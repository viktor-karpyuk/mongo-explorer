package com.kubrik.mex.security.authn;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-F1 — parser tests for the auth-backend probe. Every mechanism
 * combination + every config-subtree shape the server may emit is
 * covered here so the live probe stays predictable across 4.x → 7.x
 * without needing a real ldap / kerberos fixture.
 */
class AuthBackendProbeTest {

    @Test
    void scram_only_fixture_enables_SCRAM_256_disables_everything_else() {
        Document mech = new Document("authenticationMechanisms",
                List.of("SCRAM-SHA-256"));
        var snap = AuthBackendProbe.parse(mech, null);

        AuthBackend s256 = find(snap, AuthBackend.Mechanism.SCRAM_SHA_256);
        AuthBackend s1   = find(snap, AuthBackend.Mechanism.SCRAM_SHA_1);
        AuthBackend ldap = find(snap, AuthBackend.Mechanism.PLAIN_LDAP);
        AuthBackend gss  = find(snap, AuthBackend.Mechanism.GSSAPI);
        AuthBackend x509 = find(snap, AuthBackend.Mechanism.MONGODB_X509);

        assertTrue(s256.enabled());
        assertFalse(s1.enabled());
        assertFalse(ldap.enabled());
        assertFalse(gss.enabled());
        assertFalse(x509.enabled());
    }

    @Test
    void mixed_mechanisms_enabled_all_at_once() {
        Document mech = new Document("authenticationMechanisms",
                List.of("SCRAM-SHA-256", "SCRAM-SHA-1", "MONGODB-X509", "PLAIN", "GSSAPI"));
        var snap = AuthBackendProbe.parse(mech, null);

        for (AuthBackend.Mechanism m : AuthBackend.Mechanism.values()) {
            assertTrue(find(snap, m).enabled(), m + " must be enabled");
        }
    }

    @Test
    void ldap_config_flows_into_PLAIN_details_but_password_is_redacted() {
        Document mech = new Document("authenticationMechanisms", List.of("PLAIN"));
        Document cmdLine = new Document("parsed", new Document()
                .append("security", new Document()
                        .append("ldap", new Document()
                                .append("servers", "ldap.internal:636")
                                .append("bind", new Document()
                                        .append("method", "simple")
                                        .append("queryUser", "cn=mex,ou=services")
                                        .append("queryPassword", "shhh-hunter2")))));
        var snap = AuthBackendProbe.parse(mech, cmdLine);

        AuthBackend ldap = find(snap, AuthBackend.Mechanism.PLAIN_LDAP);
        assertTrue(ldap.enabled());
        assertEquals("ldap.internal:636", ldap.details().get("ldap.servers"));
        assertEquals("simple", ldap.details().get("ldap.bind.method"));
        assertEquals("cn=mex,ou=services", ldap.details().get("ldap.bind.queryUser"));
        // Secrets must not appear anywhere in the details map.
        assertFalse(ldap.details().containsKey("ldap.bind.queryPassword"),
                "queryPassword must be redacted");
        for (String v : ldap.details().values()) {
            assertFalse(v.contains("hunter2"), "password value leaked through a sibling key");
        }
    }

    @Test
    void kerberos_config_flows_into_GSSAPI_details() {
        Document mech = new Document("authenticationMechanisms", List.of("GSSAPI"));
        Document cmdLine = new Document("parsed", new Document()
                .append("security", new Document()
                        .append("kerberos", new Document()
                                .append("serviceName", "mongodb")
                                .append("keytab", "/etc/krb5.keytab"))));
        var snap = AuthBackendProbe.parse(mech, cmdLine);

        AuthBackend gss = find(snap, AuthBackend.Mechanism.GSSAPI);
        assertTrue(gss.enabled());
        assertEquals("mongodb", gss.details().get("kerberos.serviceName"));
        // The keytab *path* is fine — that's config, not a secret. A
        // "keypassword" field would be redacted.
        assertEquals("/etc/krb5.keytab", gss.details().get("kerberos.keytab"));
    }

    @Test
    void x509_surfaces_the_tls_certificate_paths() {
        Document mech = new Document("authenticationMechanisms", List.of("MONGODB-X509"));
        Document cmdLine = new Document("parsed", new Document()
                .append("security", new Document()
                        .append("tls", new Document()
                                .append("CAFile", "/etc/ssl/ca.pem")
                                .append("certificateKeyFilePassword", "should-not-leak"))));
        var snap = AuthBackendProbe.parse(mech, cmdLine);

        AuthBackend x509 = find(snap, AuthBackend.Mechanism.MONGODB_X509);
        assertTrue(x509.enabled());
        assertEquals("/etc/ssl/ca.pem", x509.details().get("tls.CAFile"));
        assertFalse(x509.details().keySet().stream()
                .anyMatch(k -> k.toLowerCase().contains("password")),
                "password-bearing keys must be redacted");
    }

    @Test
    void isRedacted_catches_common_secret_fields() {
        assertTrue(AuthBackendProbe.isRedacted("ldap.bind.queryPassword"));
        assertTrue(AuthBackendProbe.isRedacted("clusterAuthPassword"));
        assertTrue(AuthBackendProbe.isRedacted("tls.certificateKeyFilePassword"));
        assertTrue(AuthBackendProbe.isRedacted("security.keyFile"));
        assertFalse(AuthBackendProbe.isRedacted("ldap.servers"));
        assertFalse(AuthBackendProbe.isRedacted("kerberos.serviceName"));
    }

    @Test
    void null_replies_produce_a_snapshot_with_all_mechanisms_disabled() {
        var snap = AuthBackendProbe.parse(null, null);
        assertEquals(AuthBackend.Mechanism.values().length, snap.backends().size());
        assertTrue(snap.backends().stream().noneMatch(AuthBackend::enabled));
    }

    @Test
    void mechanism_fromWire_is_case_insensitive() {
        assertEquals(AuthBackend.Mechanism.SCRAM_SHA_256,
                AuthBackend.Mechanism.fromWire("scram-sha-256"));
        assertEquals(AuthBackend.Mechanism.GSSAPI,
                AuthBackend.Mechanism.fromWire("gssapi"));
        assertNull(AuthBackend.Mechanism.fromWire("CUSTOM"));
    }

    /* ============================== helpers ============================== */

    private static AuthBackend find(AuthBackendProbe.Snapshot s, AuthBackend.Mechanism m) {
        return s.backends().stream()
                .filter(b -> b.mechanism() == m).findFirst().orElseThrow();
    }
}
