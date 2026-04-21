package com.kubrik.mex.security.cis;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * v2.6 Q2.6-H3 — one {@code cis_suppressions} row. A suppression hides a
 * rule's FAIL verdict for a given {@code scope} (CLUSTER, HOST:&lt;h&gt;,
 * NS:&lt;db.coll&gt;) until {@code expiresAtMs} (null = no expiry). The
 * suppressed finding still appears in the report with
 * {@link CisFinding#suppressed()} = true so the auditor sees the reason.
 */
public record CisSuppression(
        long id,
        String connectionId,
        String ruleId,
        String scope,
        String reason,
        long createdAtMs,
        String createdBy,
        Long expiresAtMs
) {
    public CisSuppression {
        Objects.requireNonNull(connectionId, "connectionId");
        Objects.requireNonNull(ruleId, "ruleId");
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(reason, "reason");
        if (createdBy == null) createdBy = "";
    }

    /** Active at {@code atMs} iff not expired. */
    public boolean active(long atMs) {
        return expiresAtMs == null || atMs < expiresAtMs;
    }

    /** Find a suppression matching the {@code (ruleId, scope)} of a
     *  finding from a list. Returns the first active one. */
    public static Optional<CisSuppression> find(List<CisSuppression> all,
                                                  String ruleId, String scope,
                                                  long atMs) {
        if (all == null) return Optional.empty();
        return all.stream()
                .filter(s -> s.ruleId().equals(ruleId))
                .filter(s -> s.scope().equals(scope))
                .filter(s -> s.active(atMs))
                .findFirst();
    }
}
