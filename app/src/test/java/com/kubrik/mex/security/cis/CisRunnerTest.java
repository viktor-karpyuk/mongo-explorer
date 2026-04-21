package com.kubrik.mex.security.cis;

import com.kubrik.mex.security.access.UsersRolesFetcher;
import com.kubrik.mex.security.authn.AuthBackendProbe;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-H1 — runner behaviour: tally correctness, suppression
 * folding (FAIL + active suppression → suppressed count, not fail count),
 * verdict counting, and rule-exception isolation.
 */
class CisRunnerTest {

    @Test
    void tallies_pass_fail_not_applicable_correctly() {
        CisRunner runner = new CisRunner(List.of(
                alwaysPass("A1"),
                alwaysFail("A2"),
                alwaysNotApplicable("A3"),
                alwaysFail("A4")));

        CisReport report = runner.run(ctx(), List.of(), 1_000L);

        assertEquals(4, report.total());
        assertEquals(1, report.pass());
        assertEquals(2, report.fail());
        assertEquals(1, report.notApplicable());
        assertEquals(0, report.suppressed());
        assertFalse(report.clean());
    }

    @Test
    void active_suppression_converts_FAIL_into_suppressed_count() {
        CisRunner runner = new CisRunner(List.of(alwaysFail("CIS-2.1"), alwaysFail("CIS-2.2")));
        List<CisSuppression> supps = List.of(
                new CisSuppression(-1, "cx", "CIS-2.1", "CLUSTER",
                        "auditor-approved risk exception",
                        0L, "dba", null));

        CisReport report = runner.run(ctx(), supps, 1_000L);

        assertEquals(1, report.fail(),
                "CIS-2.1 was suppressed and should not count against fail");
        assertEquals(1, report.suppressed());
        // The suppressed finding still appears in the list with the flag
        // set — auditors need to see what was muted and why.
        CisFinding f = report.findings().stream()
                .filter(x -> x.ruleId().equals("CIS-2.1")).findFirst().orElseThrow();
        assertTrue(f.suppressed());
    }

    @Test
    void expired_suppression_does_not_apply() {
        CisRunner runner = new CisRunner(List.of(alwaysFail("CIS-2.1")));
        List<CisSuppression> supps = List.of(
                new CisSuppression(-1, "cx", "CIS-2.1", "CLUSTER",
                        "short-lived", 0L, "dba", 500L));

        CisReport after = runner.run(ctx(), supps, 1_000L);
        assertEquals(1, after.fail());
        assertEquals(0, after.suppressed(), "suppression expired at 500ms");
    }

    @Test
    void a_buggy_rule_becomes_NOT_APPLICABLE_rather_than_aborting_the_scan() {
        CisRunner runner = new CisRunner(List.of(
                alwaysPass("OK"),
                new CisRule() {
                    public String id() { return "BUGGY"; }
                    public String title() { return "Throws"; }
                    public Severity severity() { return Severity.LOW; }
                    public Evaluation evaluate(ComplianceContext ctx) {
                        throw new RuntimeException("boom");
                    }
                },
                alwaysPass("OK2")));

        CisReport report = runner.run(ctx(), List.of(), 1_000L);

        assertEquals(3, report.total());
        assertEquals(2, report.pass());
        assertEquals(1, report.notApplicable());
        CisFinding buggy = report.findings().stream()
                .filter(f -> f.ruleId().equals("BUGGY")).findFirst().orElseThrow();
        assertEquals(CisRule.Evaluation.Verdict.NOT_APPLICABLE, buggy.verdict());
        assertTrue(buggy.detail().contains("boom"));
    }

    @Test
    void empty_rule_list_produces_empty_report() {
        CisReport report = new CisRunner(List.of()).run(ctx(), List.of(), 1_000L);
        assertEquals(0, report.total());
        assertTrue(report.clean());
    }

    @Test
    void benchmark_version_is_surfaced_on_the_report() {
        CisReport report = new CisRunner(List.of()).run(ctx(), List.of(), 0L);
        assertEquals("CIS MongoDB v1.2", report.benchmarkVersion());
    }

    /* ============================== helpers ============================== */

    private static ComplianceContext ctx() {
        return new ComplianceContext("cx",
                new UsersRolesFetcher.Snapshot(List.of(), List.of()),
                new AuthBackendProbe.Snapshot(List.of(), 0L),
                List.of(),
                List.of());
    }

    private static CisRule alwaysPass(String id) {
        return new CisRule() {
            public String id() { return id; }
            public String title() { return "pass"; }
            public Severity severity() { return Severity.INFO; }
            public Evaluation evaluate(ComplianceContext ctx) { return Evaluation.pass("ok"); }
        };
    }

    private static CisRule alwaysFail(String id) {
        return new CisRule() {
            public String id() { return id; }
            public String title() { return "fail"; }
            public Severity severity() { return Severity.HIGH; }
            public Evaluation evaluate(ComplianceContext ctx) { return Evaluation.fail("bad"); }
        };
    }

    private static CisRule alwaysNotApplicable(String id) {
        return new CisRule() {
            public String id() { return id; }
            public String title() { return "na"; }
            public Severity severity() { return Severity.LOW; }
            public Evaluation evaluate(ComplianceContext ctx) {
                return Evaluation.notApplicable("no data");
            }
        };
    }
}
