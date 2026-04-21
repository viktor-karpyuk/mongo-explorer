package com.kubrik.mex.security;

import com.kubrik.mex.security.baseline.SecurityBaselineDao;
import com.kubrik.mex.security.cert.CertCacheDao;
import com.kubrik.mex.security.cert.CertRecord;
import com.kubrik.mex.security.drift.DriftAck;
import com.kubrik.mex.security.drift.DriftAckDao;
import com.kubrik.mex.security.drift.DriftDiffEngine;
import com.kubrik.mex.security.drift.DriftFinding;
import org.bson.Document;

import java.util.List;
import java.util.Map;

/**
 * v2.6 Q2.6-D4 + Q2.6-E4 — surfaces a compact {@link Summary} the
 * Welcome view consumes per connection. Computes:
 * <ul>
 *   <li>Expiring / expired cert counts from {@link CertCacheDao}, bucketed
 *       by the {@link CertRecord} expiry-band helper.</li>
 *   <li>Unacked drift count by diffing the latest baseline against the
 *       previous one and filtering via {@link DriftAck}.</li>
 * </ul>
 *
 * <p>Pure read-side — callers drive the refresh cadence. The welcome card
 * invokes this on card render; a daily background check (deferred to
 * Q2.6-K) will also call it and emit the {@code onCertExpiry} event.</p>
 */
public final class SecuritySignals {

    private SecuritySignals() {}

    public record Summary(
            int expiredCerts,
            int expiringSoonCerts,
            int unackedDrifts,
            boolean hasBaseline,
            long latestBaselineAtMs
    ) {
        public boolean clean() {
            return expiredCerts == 0 && expiringSoonCerts == 0 && unackedDrifts == 0;
        }
    }

    public static Summary compute(String connectionId,
                                    SecurityBaselineDao baselineDao,
                                    DriftAckDao ackDao,
                                    CertCacheDao certCacheDao,
                                    long nowMs) {
        int expired = 0;
        int amberRed = 0;
        if (certCacheDao != null) {
            List<CertCacheDao.Row> rows = certCacheDao.listForConnection(connectionId);
            for (CertCacheDao.Row r : rows) {
                if (r.notAfter() == null) continue;
                long remaining = r.notAfter() - nowMs;
                if (remaining < 0) expired++;
                else if (remaining < 30L * 86_400_000L) amberRed++;
            }
        }

        int unacked = 0;
        boolean hasBaseline = false;
        long latestAt = 0L;
        if (baselineDao != null) {
            List<SecurityBaselineDao.Row> recent = baselineDao.listForConnection(connectionId, 2);
            hasBaseline = !recent.isEmpty();
            if (hasBaseline) latestAt = recent.get(0).capturedAt();
            if (recent.size() >= 2 && ackDao != null) {
                unacked = countUnacked(connectionId, recent.get(1), recent.get(0), ackDao);
            }
        }

        return new Summary(expired, amberRed, unacked, hasBaseline, latestAt);
    }

    @SuppressWarnings("unchecked")
    private static int countUnacked(String connectionId,
                                      SecurityBaselineDao.Row before,
                                      SecurityBaselineDao.Row after,
                                      DriftAckDao ackDao) {
        Map<String, Object> beforeTree = parseJson(before.snapshotJson());
        Map<String, Object> afterTree = parseJson(after.snapshotJson());
        List<DriftFinding> findings = DriftDiffEngine.diff(
                (Map<String, Object>) beforeTree.getOrDefault("payload", Map.of()),
                (Map<String, Object>) afterTree.getOrDefault("payload", Map.of()));
        List<DriftAck> acks = ackDao.listForConnection(connectionId);
        return DriftAck.hideAcked(findings, after.id(), acks).size();
    }

    private static Map<String, Object> parseJson(String json) {
        if (json == null || json.isBlank()) return Map.of();
        try { return new java.util.LinkedHashMap<>(Document.parse(json)); }
        catch (Exception e) { return Map.of(); }
    }
}
