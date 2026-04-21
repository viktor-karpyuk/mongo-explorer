package com.kubrik.mex.security;

import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-A2 — evidence-signing key lifecycle. The first signer on a
 * fresh install mints the key; a second signer built against the same
 * database reuses the persisted key (same HMAC for the same payload).
 * HMACs are deterministic and hex-encoded; verify is constant-time.
 */
class EvidenceSignerTest {

    @TempDir Path home;
    private Database db;
    private Crypto crypto;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", home.toString());
        db = new Database();
        crypto = new Crypto();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void key_is_generated_on_first_instantiation_and_reused_thereafter() {
        EvidenceSigner first = new EvidenceSigner(db, crypto);
        String sig1 = first.sign("hello");

        EvidenceSigner second = new EvidenceSigner(db, crypto);
        String sig2 = second.sign("hello");

        assertEquals(sig1, sig2,
                "second signer must reuse the persisted key and produce the same HMAC");
    }

    @Test
    void hmac_is_deterministic_64_char_hex() {
        EvidenceSigner s = new EvidenceSigner(db, crypto);
        String sig = s.sign("deterministic-payload");
        assertEquals(64, sig.length(), "HMAC-SHA-256 hex must be 64 chars");
        assertTrue(sig.matches("[0-9a-f]+"), "hex-only, lower case");
        assertEquals(sig, s.sign("deterministic-payload"));
    }

    @Test
    void verify_succeeds_for_matching_payload_and_signature() {
        EvidenceSigner s = new EvidenceSigner(db, crypto);
        String sig = s.sign("report-bundle");
        assertTrue(s.verify("report-bundle", sig));
        assertFalse(s.verify("report-bundle-tampered", sig),
                "verify must fail when one byte flips");
    }

    @Test
    void verify_rejects_null_or_blank_signature() {
        EvidenceSigner s = new EvidenceSigner(db, crypto);
        assertFalse(s.verify("anything", null));
        assertFalse(s.verify("anything", ""));
        assertFalse(s.verify("anything", "   "));
    }
}
