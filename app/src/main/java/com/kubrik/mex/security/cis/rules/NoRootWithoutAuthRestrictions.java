package com.kubrik.mex.security.cis.rules;

import com.kubrik.mex.security.access.RoleBinding;
import com.kubrik.mex.security.access.UserRecord;
import com.kubrik.mex.security.cis.CisRule;
import com.kubrik.mex.security.cis.ComplianceContext;

/** CIS MongoDB v1.2 §5.2 — every {@code root}-bound user must have
 *  {@code authenticationRestrictions} scoping their logins to known
 *  client networks. Fails on any root-bound user with an empty
 *  restrictions list. */
public final class NoRootWithoutAuthRestrictions implements CisRule {
    public String id() { return "CIS-5.2"; }
    public String title() { return "root-role users carry authenticationRestrictions"; }
    public Severity severity() { return Severity.CRITICAL; }
    public String rationale() {
        return "A root user with no client-source allow-list means any "
                + "compromised endpoint on the same network can escalate. "
                + "Restrictions force the admin network path to be "
                + "explicit.";
    }
    public Evaluation evaluate(ComplianceContext ctx) {
        if (ctx.users().users().isEmpty()) {
            return Evaluation.notApplicable("no user snapshot");
        }
        int violations = 0;
        StringBuilder names = new StringBuilder();
        for (UserRecord u : ctx.users().users()) {
            boolean isRoot = u.roleBindings().stream()
                    .map(RoleBinding::role)
                    .anyMatch("root"::equals);
            if (isRoot && u.authenticationRestrictions().isEmpty()) {
                if (violations > 0) names.append(", ");
                names.append(u.fullyQualified());
                violations++;
            }
        }
        return violations == 0
                ? Evaluation.pass("Every root-role user has authentication restrictions")
                : Evaluation.fail(violations + " root user(s) without "
                        + "authenticationRestrictions: " + names);
    }
}
