package com.kubrik.mex.security.cis.rules;

import com.kubrik.mex.security.cert.CertRecord;
import com.kubrik.mex.security.cis.CisRule;
import com.kubrik.mex.security.cis.ComplianceContext;

import java.time.Clock;

/** CIS MongoDB v1.2 §2.5 (TLS). Fails when any observed cluster-member
 *  certificate is within 30 days of expiry — a 30-day runway is the CIS
 *  benchmark's minimum for controlled rotation. */
public final class CertsNotImminent implements CisRule {
    /** Overridable for tests — default uses the system clock so scan
     *  reports timestamp cleanly against the calendar. */
    private final Clock clock;

    public CertsNotImminent() { this(Clock.systemUTC()); }
    public CertsNotImminent(Clock clock) { this.clock = clock; }

    public String id() { return "CIS-2.5"; }
    public String title() { return "TLS certificates have more than 30 days to expiry"; }
    public Severity severity() { return Severity.MEDIUM; }
    public String rationale() {
        return "A cert that expires during a lull in attention causes "
                + "authenticated traffic to break (Kubernetes liveness, "
                + "secondary reconnects). 30 days is enough to notice + "
                + "rotate in a controlled window.";
    }
    public Evaluation evaluate(ComplianceContext ctx) {
        if (ctx.certs().isEmpty()) {
            return Evaluation.notApplicable("no cert inventory");
        }
        long now = clock.millis();
        long bad = ctx.certs().stream()
                .map(c -> c.expiryBand(now))
                .filter(b -> b == CertRecord.ExpiryBand.AMBER
                        || b == CertRecord.ExpiryBand.RED
                        || b == CertRecord.ExpiryBand.EXPIRED)
                .count();
        if (bad == 0) return Evaluation.pass("All certs have > 30 days");
        String hosts = ctx.certs().stream()
                .filter(c -> c.expiryBand(now) != CertRecord.ExpiryBand.GREEN)
                .map(CertRecord::host)
                .reduce((a, b) -> a + ", " + b).orElse("");
        return Evaluation.fail(bad + " cert(s) within 30 days: " + hosts);
    }
}
