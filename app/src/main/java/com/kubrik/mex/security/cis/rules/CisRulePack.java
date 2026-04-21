package com.kubrik.mex.security.cis.rules;

import com.kubrik.mex.security.cis.CisRule;

import java.util.List;

/**
 * v2.6 Q2.6-H2 — the full rule set shipped with this release. Kept as
 * a simple {@code List&lt;CisRule&gt;} so the runner can be handed
 * exactly the rules the test wants, and so future additions are a
 * one-line change (add the instance to {@link #all()}).
 *
 * <p>v2.6.0 ships the below starter pack covering each probe domain.
 * Expanding to the full CIS MongoDB v1.2 rule list is tracked as a
 * v2.6.1 follow-up; the scan is scored against whichever rules are
 * present.</p>
 */
public final class CisRulePack {

    private CisRulePack() {}

    public static List<CisRule> all() {
        return List.of(
                new RequireScramSha256(),
                new DisallowScramSha1(),
                new RequireEncryptionAtRest(),
                new CertsNotImminent(),
                new NoRootWithoutAuthRestrictions());
    }
}
