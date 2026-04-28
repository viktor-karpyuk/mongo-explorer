package com.kubrik.mex.security.cis;

import java.util.Objects;

/** v2.6 Q2.6-H1 — one row in a CIS scan report. Wraps the rule's
 *  evaluation plus the rule's own descriptive fields + a suppression
 *  marker so the pane can render Suppressed separately from Pass / Fail. */
public record CisFinding(
        String ruleId,
        String title,
        CisRule.Severity severity,
        CisRule.Evaluation.Verdict verdict,
        String detail,
        String scope,
        boolean suppressed
) {
    public CisFinding {
        Objects.requireNonNull(ruleId, "ruleId");
        Objects.requireNonNull(title, "title");
        Objects.requireNonNull(severity, "severity");
        Objects.requireNonNull(verdict, "verdict");
        if (detail == null) detail = "";
        if (scope == null) scope = "CLUSTER";
    }
}
