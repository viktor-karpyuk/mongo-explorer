package com.kubrik.mex.backup.sink;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6.1 Q2.6.1-C — SFTP URI + credential classifier tests. Live SFTP
 * round-trip against an in-process Apache MINA SSHD server is
 * deferred to the smoke pass.
 */
class SftpTargetTest {

    /* =========================== parseUri =========================== */

    @Test
    void parses_user_at_host_with_default_port_and_path() {
        SftpTarget.Parsed p = SftpTarget.parseUri("sftp://backup@h.example.com/var/backups");
        assertEquals("backup", p.user());
        assertEquals("h.example.com", p.host());
        assertEquals(22, p.port());
        assertEquals("var/backups", p.rootPath());
    }

    @Test
    void parses_custom_port() {
        SftpTarget.Parsed p = SftpTarget.parseUri("sftp://bkp@h.example.com:2222/var/backups");
        assertEquals(2222, p.port());
        assertEquals("h.example.com", p.host());
    }

    @Test
    void path_defaults_to_dot_when_absent() {
        SftpTarget.Parsed p = SftpTarget.parseUri("sftp://bkp@h.example.com");
        assertEquals(".", p.rootPath());
    }

    @Test
    void trailing_slash_with_no_path_becomes_dot() {
        SftpTarget.Parsed p = SftpTarget.parseUri("sftp://bkp@h.example.com/");
        assertEquals(".", p.rootPath());
    }

    @Test
    void rejects_blank_or_wrong_scheme() {
        assertThrows(IllegalArgumentException.class,
                () -> SftpTarget.parseUri(""));
        assertThrows(IllegalArgumentException.class,
                () -> SftpTarget.parseUri(null));
        assertThrows(IllegalArgumentException.class,
                () -> SftpTarget.parseUri("ftp://bkp@h/path"));
    }

    @Test
    void rejects_missing_user() {
        assertThrows(IllegalArgumentException.class,
                () -> SftpTarget.parseUri("sftp://h.example.com/path"));
    }

    @Test
    void rejects_invalid_port() {
        assertThrows(IllegalArgumentException.class,
                () -> SftpTarget.parseUri("sftp://bkp@h:not-a-number/path"));
    }

    @Test
    void rejects_password_embedded_in_userinfo() {
        // sftp://user:password@host would both break JSch (which
        // expects a bare username) AND put the password at risk of
        // leaking into log / status strings. Parser rejects with a
        // specific hint to use the credentials field.
        IllegalArgumentException bad = assertThrows(
                IllegalArgumentException.class,
                () -> SftpTarget.parseUri("sftp://bkp:secret@h:22/path"));
        assertTrue(bad.getMessage().toLowerCase().contains("password"));
    }

    @Test
    void strips_query_and_fragment_from_pasted_url() {
        SftpTarget.Parsed p = SftpTarget.parseUri("sftp://bkp@h/path?ref=1#x");
        assertEquals("path", p.rootPath());
    }

    /* ======================== classifyCredentials ======================== */

    @Test
    void unauthenticated_for_blank_credentials() {
        assertEquals(SftpTarget.AuthKind.UNAUTHENTICATED,
                SftpTarget.classifyCredentials(null));
        assertEquals(SftpTarget.AuthKind.UNAUTHENTICATED,
                SftpTarget.classifyCredentials(""));
        assertEquals(SftpTarget.AuthKind.UNAUTHENTICATED,
                SftpTarget.classifyCredentials("   "));
    }

    @Test
    void password_classification() {
        assertEquals(SftpTarget.AuthKind.PASSWORD,
                SftpTarget.classifyCredentials("{\"password\":\"hunter2\"}"));
    }

    @Test
    void private_key_classification() {
        assertEquals(SftpTarget.AuthKind.PRIVATE_KEY,
                SftpTarget.classifyCredentials(
                        "{\"privateKey\":\"-----BEGIN OPENSSH PRIVATE KEY-----\\n…\"}"));
    }

    @Test
    void private_key_with_passphrase_still_classifies_as_private_key() {
        assertEquals(SftpTarget.AuthKind.PRIVATE_KEY,
                SftpTarget.classifyCredentials(
                        "{\"privateKey\":\"-----BEGIN …\",\"passphrase\":\"secret\"}"));
    }

    @Test
    void private_key_wins_over_password_when_both_present() {
        // Admin pasted both — key is stronger, prefer it.
        assertEquals(SftpTarget.AuthKind.PRIVATE_KEY,
                SftpTarget.classifyCredentials(
                        "{\"password\":\"hunter2\",\"privateKey\":\"-----BEGIN …\"}"));
    }

    @Test
    void malformed_json_returns_INVALID() {
        assertEquals(SftpTarget.AuthKind.INVALID,
                SftpTarget.classifyCredentials("not json"));
        assertEquals(SftpTarget.AuthKind.INVALID,
                SftpTarget.classifyCredentials("{\"unknown\":\"field\"}"));
    }
}
