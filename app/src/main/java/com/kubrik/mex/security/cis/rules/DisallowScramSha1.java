package com.kubrik.mex.security.cis.rules;

import com.kubrik.mex.security.authn.AuthBackend;
import com.kubrik.mex.security.cis.CisRule;
import com.kubrik.mex.security.cis.ComplianceContext;

/** CIS MongoDB v1.2 §6.1.4 — fail when SCRAM-SHA-1 is still advertised.
 *  Paired with {@link RequireScramSha256}; this one focuses on the
 *  "remove the weaker option" side of the same policy. */
public final class DisallowScramSha1 implements CisRule {
    public String id() { return "CIS-6.1.4"; }
    public String title() { return "Disable SCRAM-SHA-1 auth mechanism"; }
    public Severity severity() { return Severity.MEDIUM; }
    public String rationale() {
        return "SCRAM-SHA-1 is retained for legacy driver compatibility. "
                + "Once every client supports SCRAM-SHA-256, remove it "
                + "from authenticationMechanisms to stop down-negotiation.";
    }
    public Evaluation evaluate(ComplianceContext ctx) {
        boolean has1 = ctx.auth().backends().stream()
                .anyMatch(b -> b.mechanism() == AuthBackend.Mechanism.SCRAM_SHA_1
                        && b.enabled());
        return has1
                ? Evaluation.fail("SCRAM-SHA-1 is still advertised alongside "
                        + "SCRAM-SHA-256 — consider removing it")
                : Evaluation.pass("SCRAM-SHA-1 is not enabled");
    }
}
