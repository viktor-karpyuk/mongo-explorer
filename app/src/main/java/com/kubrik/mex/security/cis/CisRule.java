package com.kubrik.mex.security.cis;

/**
 * v2.6 Q2.6-H1 — one rule in the CIS MongoDB Benchmark. Rules are pure
 * functions of the {@link ComplianceContext}: no I/O, no clock, no
 * randomness. This lets the runner fold every rule in parallel (Q2.6-H2
 * later), and lets each rule be unit-tested in isolation with a fixture
 * context.
 *
 * <p>Implementations must return exactly one {@link Evaluation} per
 * call. Use {@link Evaluation.Verdict#NOT_APPLICABLE} when the rule
 * doesn't have the data it needs — it's the honest answer, not a
 * failure. {@code PASS} / {@code FAIL} are used when the rule can make
 * a call.</p>
 */
public interface CisRule {

    /** Stable identifier — matches the CIS benchmark numbering
     *  (e.g. {@code "CIS-2.1"}, {@code "CIS-2.5"}). The suppression
     *  model and the ack workflow reference rules by this id. */
    String id();

    /** One-line title shown in the UI + report. */
    String title();

    Severity severity();

    /** Human-readable remediation or "why this matters" text. */
    default String rationale() { return ""; }

    /** Deterministic evaluation. */
    Evaluation evaluate(ComplianceContext ctx);

    enum Severity { INFO, LOW, MEDIUM, HIGH, CRITICAL }

    record Evaluation(Verdict verdict, String detail, String scope) {

        public Evaluation {
            if (verdict == null) throw new IllegalArgumentException("verdict");
            if (detail == null) detail = "";
            if (scope == null) scope = "CLUSTER";
        }

        public static Evaluation pass(String detail) {
            return new Evaluation(Verdict.PASS, detail, "CLUSTER");
        }

        public static Evaluation fail(String detail) {
            return new Evaluation(Verdict.FAIL, detail, "CLUSTER");
        }

        public static Evaluation notApplicable(String detail) {
            return new Evaluation(Verdict.NOT_APPLICABLE, detail, "CLUSTER");
        }

        public enum Verdict { PASS, FAIL, NOT_APPLICABLE }
    }
}
