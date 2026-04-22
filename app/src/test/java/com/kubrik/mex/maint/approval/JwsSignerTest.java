package com.kubrik.mex.maint.approval;

import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.security.EvidenceSigner;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 Q2.7-A — Tamper + round-trip coverage for the minimal HS256
 * producer. Narrow by design; the library isn't meant to be general.
 */
class JwsSignerTest {

    @TempDir Path dataDir;

    private Database db;
    private JwsSigner jws;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        jws = new JwsSigner(new EvidenceSigner(db, new Crypto()));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void round_trip_preserves_claims() {
        Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("a", "rcfg.apply");
        claims.put("rev", "bob");
        claims.put("iat", 1_700_000_000L);
        claims.put("ok", Boolean.TRUE);
        claims.put("nothing", null);

        String token = jws.sign(claims);
        Optional<Map<String, Object>> verified = jws.verify(token);
        assertTrue(verified.isPresent());
        Map<String, Object> got = verified.get();
        assertEquals("rcfg.apply", got.get("a"));
        assertEquals("bob", got.get("rev"));
        assertEquals(1_700_000_000L, got.get("iat"));
        assertEquals(Boolean.TRUE, got.get("ok"));
        assertNull(got.get("nothing"));
    }

    @Test
    void verify_rejects_malformed_token() {
        assertTrue(jws.verify(null).isEmpty());
        assertTrue(jws.verify("not.a.jws").isEmpty());
        assertTrue(jws.verify("missing.dot").isEmpty());
        assertTrue(jws.verify("a.b.c.d").isEmpty());
    }

    @Test
    void verify_rejects_tampered_payload() {
        Map<String, Object> claims = Map.of("a", "rcfg.apply");
        String token = jws.sign(claims);
        // Flip the payload segment (middle). Signature covers the
        // header + payload so any byte flip here breaks verify.
        String[] parts = token.split("\\.");
        String tamperedPayload = parts[1].substring(0, parts[1].length() - 2)
                + (parts[1].charAt(parts[1].length() - 2) == 'X' ? "YY" : "XX");
        String tampered = parts[0] + "." + tamperedPayload + "." + parts[2];
        assertTrue(jws.verify(tampered).isEmpty());
    }

    @Test
    void verify_rejects_wrong_header_alg() {
        // Handcraft a JWS with alg=none. Attacks of the shape "strip
        // the signature and set alg=none" are a historical JWS foot-
        // gun; a fixed header check forecloses them.
        String headerB64 = java.util.Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString("{\"alg\":\"none\",\"typ\":\"JWT\"}".getBytes());
        String payloadB64 = java.util.Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString("{\"a\":\"hax\"}".getBytes());
        String forged = headerB64 + "." + payloadB64 + ".";
        assertTrue(jws.verify(forged).isEmpty());
    }
}
