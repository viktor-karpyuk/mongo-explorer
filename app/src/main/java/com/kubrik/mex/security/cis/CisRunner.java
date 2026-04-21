package com.kubrik.mex.security.cis;

import java.util.ArrayList;
import java.util.List;

/**
 * v2.6 Q2.6-H1 — folds every {@link CisRule} over a
 * {@link ComplianceContext}, applies any active {@link CisSuppression}
 * rows, and produces a scored {@link CisReport}.
 *
 * <p>The runner is stateless — pass it a fresh context + rule list +
 * suppressions on each invocation. Parallel evaluation is intentionally
 * left out of v2.6; rule counts are in the low dozens and the
 * bottleneck is the probes, not the fold.</p>
 */
public final class CisRunner {

    public static final String BENCHMARK_VERSION = "CIS MongoDB v1.2";

    private final List<CisRule> rules;

    public CisRunner(List<CisRule> rules) {
        this.rules = rules == null ? List.of() : List.copyOf(rules);
    }

    public CisReport run(ComplianceContext ctx, List<CisSuppression> suppressions,
                          long nowMs) {
        List<CisFinding> findings = new ArrayList<>(rules.size());
        int pass = 0, fail = 0, na = 0, suppressed = 0;

        for (CisRule rule : rules) {
            CisRule.Evaluation eval = safeEvaluate(rule, ctx);
            boolean sup = eval.verdict() == CisRule.Evaluation.Verdict.FAIL
                    && CisSuppression.find(suppressions, rule.id(), eval.scope(), nowMs).isPresent();

            CisFinding finding = new CisFinding(rule.id(), rule.title(),
                    rule.severity(), eval.verdict(), eval.detail(),
                    eval.scope(), sup);
            findings.add(finding);

            switch (eval.verdict()) {
                case PASS -> pass++;
                case NOT_APPLICABLE -> na++;
                case FAIL -> {
                    if (sup) suppressed++;
                    else fail++;
                }
            }
        }

        return new CisReport(ctx.connectionId(), nowMs, BENCHMARK_VERSION,
                findings, pass, fail, na, suppressed);
    }

    /** Rule bugs must not break the scan. Turn any unchecked exception
     *  into a NOT_APPLICABLE with a clear message so the operator can
     *  see which rule misbehaved without the whole run aborting. */
    private static CisRule.Evaluation safeEvaluate(CisRule rule, ComplianceContext ctx) {
        try {
            CisRule.Evaluation e = rule.evaluate(ctx);
            return e == null
                    ? CisRule.Evaluation.notApplicable("rule returned null")
                    : e;
        } catch (Exception ex) {
            return CisRule.Evaluation.notApplicable(
                    "rule threw " + ex.getClass().getSimpleName()
                    + ": " + ex.getMessage());
        }
    }
}
