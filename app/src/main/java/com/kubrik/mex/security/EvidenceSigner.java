package com.kubrik.mex.security;

import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.store.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;

/**
 * v2.6 Q2.6-A2 — HMAC-SHA-256 signer for exported evidence bundles
 * (CIS scan reports, drift diffs, cert inventory JSON, audit search
 * exports). The signing key is generated once per install on first use,
 * AES-wrapped via {@link Crypto}, and persisted in the singleton
 * {@code evidence_key} row.
 *
 * <p>This key is deliberately distinct from {@link Crypto}'s per-install
 * AES key: signed reports are meant to be handed to auditors, so the
 * signing material must stay inside the install even if the exported
 * evidence leaves it. The HMAC is <em>tamper-evidence</em>, not an
 * authorship guarantee — documented in the verifier UI copy.</p>
 */
public final class EvidenceSigner {

    private static final Logger log = LoggerFactory.getLogger(EvidenceSigner.class);
    private static final String MAC_ALG = "HmacSHA256";
    private static final int KEY_LEN = 32;

    private final Database db;
    private final Crypto crypto;
    private final SecretKeySpec key;

    public EvidenceSigner(Database db, Crypto crypto) {
        this.db = db;
        this.crypto = crypto;
        this.key = loadOrGenerateKey();
    }

    /** Returns the hex-encoded HMAC-SHA-256 of {@code payload}. */
    public String sign(byte[] payload) {
        try {
            Mac mac = Mac.getInstance(MAC_ALG);
            mac.init(key);
            byte[] digest = mac.doFinal(payload);
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException("HMAC-SHA-256 unavailable", e);
        }
    }

    /** Convenience: sign a UTF-8 string payload. */
    public String sign(String payload) {
        return sign(payload.getBytes(StandardCharsets.UTF_8));
    }

    /** Constant-time comparison. Returns {@code true} iff
     *  {@link #sign(byte[])} of {@code payload} equals {@code expected}. */
    public boolean verify(byte[] payload, String expected) {
        if (expected == null || expected.isBlank()) return false;
        String actual = sign(payload);
        return MessageDigest.isEqual(
                actual.getBytes(StandardCharsets.US_ASCII),
                expected.getBytes(StandardCharsets.US_ASCII));
    }

    public boolean verify(String payload, String expected) {
        return verify(payload.getBytes(StandardCharsets.UTF_8), expected);
    }

    /* ============================ internals ============================ */

    private SecretKeySpec loadOrGenerateKey() {
        synchronized (db.writeLock()) {
            byte[] raw = readExisting();
            if (raw == null) {
                raw = new byte[KEY_LEN];
                new SecureRandom().nextBytes(raw);
                persist(raw);
                log.info("evidence-signing key generated on first run");
            }
            return new SecretKeySpec(raw, MAC_ALG);
        }
    }

    private byte[] readExisting() {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT wrapped_key FROM evidence_key WHERE id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                byte[] wrapped = rs.getBytes(1);
                if (wrapped == null) return null;
                String b64 = new String(wrapped, StandardCharsets.UTF_8);
                String decoded = crypto.decrypt(b64);
                if (decoded == null) return null;
                byte[] keyBytes = Base64.getDecoder().decode(decoded);
                if (keyBytes.length != KEY_LEN) {
                    throw new IllegalStateException(
                            "evidence key length mismatch — expected "
                                    + KEY_LEN + " bytes, got " + keyBytes.length);
                }
                return keyBytes;
            }
        } catch (SQLException e) {
            throw new RuntimeException("evidence_key read failed", e);
        }
    }

    private void persist(byte[] raw) {
        String b64 = Base64.getEncoder().encodeToString(raw);
        String enc = crypto.encrypt(b64);
        if (enc == null) throw new IllegalStateException("crypto.encrypt returned null");
        try (PreparedStatement ps = db.connection().prepareStatement(
                "INSERT INTO evidence_key(id, wrapped_key, created_at) VALUES (1, ?, ?)")) {
            ps.setBytes(1, enc.getBytes(StandardCharsets.UTF_8));
            ps.setLong(2, System.currentTimeMillis());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("evidence_key insert failed", e);
        }
    }
}
