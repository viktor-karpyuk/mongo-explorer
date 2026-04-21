package com.kubrik.mex.security.cert;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-E1 — CertRecord math + parser-helper pinning. The full
 * X509 → CertRecord conversion (CertFetcher.toRecord) is integration-
 * tested against a live TLS fixture in Q2.6-K; the pure-logic slices
 * (DN parsing, SHA-256, expiry bands) are unit-covered here.
 */
class CertRecordTest {

    @Test
    void fingerprint_must_be_64_hex_chars() {
        assertThrows(IllegalArgumentException.class, () ->
                new CertRecord("h", "cn", "issuer", List.of(), 0, 1, "ab", "too-short"));
    }

    @Test
    void sha256_of_empty_is_the_well_known_digest() {
        // echo -n '' | sha256sum → e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                CertFetcher.hashUtf8(""));
    }

    @Test
    void sha256_of_abc_matches_the_reference_digest() {
        // echo -n 'abc' | sha256sum → ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
        assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                CertFetcher.hashUtf8("abc"));
    }

    @Test
    void expiry_band_GREEN_when_more_than_30_days_out() {
        long now = 1_000_000_000_000L;
        CertRecord c = okCert(now, now + 365L * 86_400_000L);
        assertEquals(CertRecord.ExpiryBand.GREEN, c.expiryBand(now));
    }

    @Test
    void expiry_band_AMBER_at_30_days_or_less() {
        long now = 1_000_000_000_000L;
        CertRecord c = okCert(now, now + 25L * 86_400_000L);
        assertEquals(CertRecord.ExpiryBand.AMBER, c.expiryBand(now));
    }

    @Test
    void expiry_band_RED_at_seven_days_or_less() {
        long now = 1_000_000_000_000L;
        CertRecord c = okCert(now, now + 3L * 86_400_000L);
        assertEquals(CertRecord.ExpiryBand.RED, c.expiryBand(now));
    }

    @Test
    void expiry_band_EXPIRED_after_notAfter() {
        long now = 1_000_000_000_000L;
        CertRecord c = okCert(now - 10L * 86_400_000L, now - 86_400_000L);
        assertEquals(CertRecord.ExpiryBand.EXPIRED, c.expiryBand(now));
    }

    @Test
    void msUntilExpiry_is_negative_when_expired() {
        long now = 1_000_000_000_000L;
        CertRecord c = okCert(now - 10L * 86_400_000L, now - 86_400_000L);
        assertTrue(c.msUntilExpiry(now) < 0);
    }

    @Test
    void band_boundary_at_exactly_seven_days_is_RED() {
        // Boundary: the docs say "≤ 7 days" is RED, "> 30 days" is GREEN.
        // Exactly 7 × 24 h remaining → floorDiv → 7 → RED.
        long now = 1_000_000_000_000L;
        CertRecord c = okCert(now, now + 7L * 86_400_000L);
        assertEquals(CertRecord.ExpiryBand.RED, c.expiryBand(now));
    }

    @Test
    void band_boundary_at_exactly_thirty_days_is_AMBER() {
        long now = 1_000_000_000_000L;
        CertRecord c = okCert(now, now + 30L * 86_400_000L);
        assertEquals(CertRecord.ExpiryBand.AMBER, c.expiryBand(now));
    }

    private static CertRecord okCert(long notBefore, long notAfter) {
        return new CertRecord("host:27017", "cn", "issuer",
                List.of("DNS:host"), notBefore, notAfter,
                "deadbeef",
                "0000000000000000000000000000000000000000000000000000000000000000");
    }
}
