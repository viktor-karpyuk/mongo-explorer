package com.kubrik.mex.maint.upgrade;

import com.kubrik.mex.maint.model.UpgradePlan;
import com.kubrik.mex.maint.model.UpgradePlan.Severity;
import com.kubrik.mex.maint.model.UpgradePlan.Version;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class UpgradeScannerTest {

    private final UpgradeScanner scanner = new UpgradeScanner();

    @Test
    void version_parsing_handles_patch_and_suffix() {
        assertEquals(new Version(7, 0, 5), Version.parse("7.0.5"));
        assertEquals(new Version(6, 0, 0), Version.parse("6.0"));
        assertEquals(new Version(4, 4, 28), Version.parse("4.4.28-rc0"));
    }

    @Test
    void wide_major_gap_is_blocked() {
        UpgradeRules.Context ctx = new UpgradeRules.Context(
                Version.parse("4.4"), Version.parse("7.0"),
                List.of(), List.of());
        UpgradePlan.Plan plan = scanner.scan(ctx, List.of("h1", "h2", "h3"));
        assertTrue(plan.hasBlockers());
        assertTrue(plan.findings().stream()
                .anyMatch(f -> f.code().equals("VERSION-GAP-TOO-WIDE")));
    }

    @Test
    void downgrade_is_blocked() {
        UpgradeRules.Context ctx = new UpgradeRules.Context(
                Version.parse("7.0"), Version.parse("6.0"),
                List.of(), List.of());
        UpgradePlan.Plan plan = scanner.scan(ctx, List.of("h1"));
        assertTrue(plan.hasBlockers());
        assertTrue(plan.findings().stream()
                .anyMatch(f -> f.code().equals("DOWNGRADE")));
    }

    @Test
    void failIndexKeyTooLong_flagged_when_moving_to_5_0() {
        UpgradeRules.Context ctx = new UpgradeRules.Context(
                Version.parse("4.4"), Version.parse("5.0"),
                List.of(), List.of("failIndexKeyTooLong"));
        UpgradePlan.Plan plan = scanner.scan(ctx, List.of("h1"));
        assertTrue(plan.findings().stream()
                .anyMatch(f -> f.code().equals("PARAM-REMOVED-failIndexKeyTooLong")));
    }

    @Test
    void fcv_step_findings_always_present() {
        UpgradePlan.Plan plan = scanner.scan(
                new UpgradeRules.Context(Version.parse("6.0"),
                        Version.parse("7.0"), List.of(), List.of()),
                List.of("h1", "h2", "h3"));
        List<String> codes = plan.findings().stream()
                .map(UpgradePlan.Finding::code).toList();
        assertTrue(codes.contains("FCV-REQUIRED"));
        assertTrue(codes.contains("FCV-RAISE"));
    }

    @Test
    void runbook_steps_end_with_post_check_and_include_fcv_raise() {
        UpgradePlan.Plan plan = scanner.scan(
                new UpgradeRules.Context(Version.parse("6.0"),
                        Version.parse("7.0"), List.of(), List.of()),
                List.of("h1", "h2", "h3"));
        List<UpgradePlan.Step> steps = plan.steps();
        assertEquals(UpgradePlan.StepKind.PRE_CHECK, steps.get(0).kind());
        assertEquals(UpgradePlan.StepKind.POST_CHECK,
                steps.get(steps.size() - 1).kind());
        assertTrue(steps.stream()
                .anyMatch(s -> s.kind() == UpgradePlan.StepKind.FCV_RAISE));
        // Exactly 2 secondary binary swaps for a 3-node cluster, plus
        // step-down + primary swap = 3 binary swaps total + 1 rolling
        // restart.
        long swaps = steps.stream()
                .filter(s -> s.kind() == UpgradePlan.StepKind.BINARY_SWAP)
                .count();
        assertEquals(3, swaps);
    }

    @Test
    void blocking_finding_still_yields_steps_for_review() {
        // Even when there's a blocker, the plan still contains the
        // steps so the UI can preview what the DBA would have to do.
        UpgradePlan.Plan plan = scanner.scan(
                new UpgradeRules.Context(Version.parse("4.4"),
                        Version.parse("7.0"), List.of(), List.of()),
                List.of("h1"));
        assertTrue(plan.hasBlockers());
        assertFalse(plan.steps().isEmpty());
    }

    @Test
    void severity_and_finding_kind_classifiers_are_consistent() {
        UpgradePlan.Plan plan = scanner.scan(
                new UpgradeRules.Context(Version.parse("4.4"),
                        Version.parse("5.0"), List.of(),
                        List.of("failIndexKeyTooLong")),
                List.of("h1", "h2"));
        UpgradePlan.Finding paramRemoved = plan.findings().stream()
                .filter(f -> f.kind() == UpgradePlan.FindingKind.PARAM_REMOVED)
                .findFirst().orElseThrow();
        assertEquals(Severity.WARN, paramRemoved.severity());
    }
}
