package com.kubrik.mex.security.cis.rules;

import com.kubrik.mex.security.authn.AuthBackend;
import com.kubrik.mex.security.cis.CisRule;
import com.kubrik.mex.security.cis.ComplianceContext;

/**
 * CIS MongoDB v1.2 §6.1.3 — ensure {@code SCRAM-SHA-256} is enabled.
 * Deprecated mechanisms ({@code SCRAM-SHA-1}, plain passwords over
 * unencrypted channels) must not be the only option.
 */
public final class RequireScramSha256 implements CisRule {
    public String id() { return "CIS-6.1.3"; }
    public String title() { return "Require SCRAM-SHA-256 auth mechanism"; }
    public Severity severity() { return Severity.HIGH; }
    public String rationale() {
        return "SCRAM-SHA-256 resists offline password attacks better than "
                + "SCRAM-SHA-1. Disabling SCRAM-SHA-1 entirely is ideal once "
                + "all clients support SCRAM-SHA-256.";
    }
    public Evaluation evaluate(ComplianceContext ctx) {
        boolean has256 = ctx.auth().backends().stream()
                .anyMatch(b -> b.mechanism() == AuthBackend.Mechanism.SCRAM_SHA_256
                        && b.enabled());
        return has256
                ? Evaluation.pass("SCRAM-SHA-256 is enabled")
                : Evaluation.fail("SCRAM-SHA-256 is not advertised — "
                        + "add it to authenticationMechanisms");
    }
}
