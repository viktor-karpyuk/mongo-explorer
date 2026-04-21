package com.kubrik.mex.security.cert;

import java.util.Objects;

/**
 * v2.6 Q2.6-E3 — emitted by {@link CertExpiryScheduler} after each
 * sweep so the welcome card can re-render the security chip without
 * the operator opening the Security tab.
 *
 * @param connectionId  the cluster the sweep ran against
 * @param sweptAtMs     epoch-ms when the sweep completed
 * @param expired       count of certs past notAfter
 * @param expiringSoon  count of certs within the 30-day window
 */
public record CertExpiryEvent(
        String connectionId,
        long sweptAtMs,
        int expired,
        int expiringSoon
) {
    public CertExpiryEvent {
        Objects.requireNonNull(connectionId, "connectionId");
    }
}
