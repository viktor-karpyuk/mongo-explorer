package com.kubrik.mex.security.cert;

import java.util.List;
import java.util.Objects;

/**
 * v2.6 Q2.6-E1 — parsed X.509 server certificate slice. Matches the
 * {@code sec_cert_cache} schema from the milestone §3.1.
 *
 * @param host           cluster-member address the cert was observed on
 * @param subjectCn      Subject CommonName
 * @param issuerCn       Issuer CommonName
 * @param sans           Subject Alternative Names (DNS + IP entries)
 * @param notBefore      validity window start (epoch-ms)
 * @param notAfter       validity window end (epoch-ms) — drives the
 *                       expiry badge on the welcome card
 * @param serialHex      hex-encoded certificate serial
 * @param fingerprintSha256  lower-case hex SHA-256 over the DER-encoded
 *                       cert; unique key in the cache so reprobing the
 *                       same cert updates the {@code captured_at} timestamp
 *                       only
 */
public record CertRecord(
        String host,
        String subjectCn,
        String issuerCn,
        List<String> sans,
        long notBefore,
        long notAfter,
        String serialHex,
        String fingerprintSha256
) {

    public CertRecord {
        Objects.requireNonNull(host, "host");
        if (subjectCn == null) subjectCn = "";
        if (issuerCn == null) issuerCn = "";
        sans = sans == null ? List.of() : List.copyOf(sans);
        if (serialHex == null) serialHex = "";
        if (fingerprintSha256 == null || fingerprintSha256.length() != 64) {
            throw new IllegalArgumentException("fingerprintSha256 must be 64 hex chars");
        }
    }

    /** Milliseconds until {@link #notAfter}; negative when already expired. */
    public long msUntilExpiry(long nowMs) { return notAfter - nowMs; }

    /** Convenience for the welcome-card badge: {@code > 30 d} green,
     *  {@code ≤ 30 d} amber, {@code ≤ 7 d} red, {@code ≤ 0} expired. */
    public ExpiryBand expiryBand(long nowMs) {
        long days = Math.floorDiv(msUntilExpiry(nowMs), 86_400_000L);
        if (days < 0)  return ExpiryBand.EXPIRED;
        if (days <= 7) return ExpiryBand.RED;
        if (days <= 30) return ExpiryBand.AMBER;
        return ExpiryBand.GREEN;
    }

    public enum ExpiryBand { GREEN, AMBER, RED, EXPIRED }
}
