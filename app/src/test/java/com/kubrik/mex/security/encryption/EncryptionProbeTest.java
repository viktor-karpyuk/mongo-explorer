package com.kubrik.mex.security.encryption;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-G1 — parse fixture serverStatus / getCmdLineOpts pairs
 * end-to-end. Covers: full serverStatus reply with KMIP keystore,
 * Vault variant, local-file fallback, getCmdLineOpts-only path, and
 * the "no encryption anywhere" default.
 */
class EncryptionProbeTest {

    @Test
    void serverStatus_with_KMIP_produces_enabled_KMIP_status() {
        Document serverStatus = new Document("encryptionAtRest", new Document()
                .append("encryptionEnabled", true)
                .append("cipher", "AES256-CBC")
                .append("kmip", new Document("server", "kmip.internal:5696"))
                .append("keyRotatedAt", 1_700_000_000_000L));

        EncryptionStatus s = EncryptionProbe.parse("rs01/h1:27017", serverStatus, null);

        assertTrue(s.enabled());
        assertEquals("wiredTiger", s.engine());
        assertEquals(EncryptionStatus.Keystore.KMIP, s.keystore());
        assertEquals("AES256-CBC", s.cipher());
        assertEquals(1_700_000_000_000L, s.rotatedAtMs());
        assertEquals("serverStatus", s.sourceCommand());
    }

    @Test
    void serverStatus_with_Vault_detects_vault_keystore() {
        Document serverStatus = new Document("encryptionAtRest", new Document()
                .append("encryptionEnabled", true)
                .append("vault", new Document("url", "https://vault.internal:8200")));
        EncryptionStatus s = EncryptionProbe.parse("h:27017", serverStatus, null);
        assertEquals(EncryptionStatus.Keystore.VAULT, s.keystore());
    }

    @Test
    void serverStatus_with_localKeyFile_detects_local_file_keystore() {
        Document serverStatus = new Document("encryptionAtRest", new Document()
                .append("encryptionEnabled", true)
                .append("localKeyFile", new Document("path", "/etc/mongodb/keys.pem")));
        EncryptionStatus s = EncryptionProbe.parse("h:27017", serverStatus, null);
        assertEquals(EncryptionStatus.Keystore.LOCAL_FILE, s.keystore());
    }

    @Test
    void falls_back_to_getCmdLineOpts_when_serverStatus_has_no_encryption_section() {
        Document cmdLine = new Document("parsed", new Document()
                .append("security", new Document()
                        .append("enableEncryption", true)
                        .append("encryptionCipherMode", "AES256-GCM")
                        .append("encryptionKeyFile", "/etc/mongodb/keys.pem")));
        EncryptionStatus s = EncryptionProbe.parse("h:27017", null, cmdLine);

        assertTrue(s.enabled());
        assertEquals("wiredTiger", s.engine());
        assertEquals(EncryptionStatus.Keystore.LOCAL_FILE, s.keystore());
        assertEquals("AES256-GCM", s.cipher());
        assertEquals("getCmdLineOpts", s.sourceCommand());
    }

    @Test
    void absent_signals_everywhere_produces_disabled_status() {
        EncryptionStatus s = EncryptionProbe.parse("h:27017", null, null);
        assertFalse(s.enabled());
        assertEquals(EncryptionStatus.Keystore.NONE, s.keystore());
        assertNull(s.rotatedAtMs());
    }

    @Test
    void rotation_timestamp_accepts_Date_as_well_as_long() {
        Document serverStatus = new Document("encryptionAtRest", new Document()
                .append("encryptionEnabled", true)
                .append("rotatedAt", new Date(1_700_000_000_000L))
                .append("kmip", new Document("server", "kmip.internal:5696")));
        EncryptionStatus s = EncryptionProbe.parse("h:27017", serverStatus, null);
        assertEquals(1_700_000_000_000L, s.rotatedAtMs());
    }

    @Test
    void keyStoreType_string_is_normalised_into_the_enum() {
        Document serverStatus = new Document("encryptionAtRest", new Document()
                .append("encryptionEnabled", true)
                .append("keyStoreType", "KMIP2.0"));
        EncryptionStatus s = EncryptionProbe.parse("h:27017", serverStatus, null);
        assertEquals(EncryptionStatus.Keystore.KMIP, s.keystore());
    }

    @Test
    void enabled_alias_on_serverStatus_is_honoured() {
        // Some MongoDB versions use `enabled` instead of `encryptionEnabled`.
        Document serverStatus = new Document("encryptionAtRest", new Document()
                .append("enabled", true)
                .append("kmip", new Document("server", "k.internal:5696")));
        EncryptionStatus s = EncryptionProbe.parse("h:27017", serverStatus, null);
        assertTrue(s.enabled());
    }
}
