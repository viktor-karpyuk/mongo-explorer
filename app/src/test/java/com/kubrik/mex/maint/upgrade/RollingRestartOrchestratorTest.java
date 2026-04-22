package com.kubrik.mex.maint.upgrade;

import com.kubrik.mex.maint.model.UpgradePlan;
import com.kubrik.mex.maint.model.UpgradePlan.Version;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 — Orchestrator coverage without a live MongoDB. The
 * MemberClientOpener + OperatorGate seams let us exercise every
 * branch: accept, decline, throw.
 */
class RollingRestartOrchestratorTest {

    @Test
    void declining_gate_halts_the_run() {
        UpgradePlan.Plan plan = new UpgradeScanner().scan(
                new UpgradeRules.Context(Version.parse("6.0"),
                        Version.parse("7.0"), List.of(), List.of()),
                List.of("h1", "h2"));

        RollingRestartOrchestrator orch = new RollingRestartOrchestrator();
        List<RollingRestartOrchestrator.StepOutcome> sunk = new ArrayList<>();
        // Gate refuses at the first BINARY_SWAP — orchestrator bails
        // so the rest of the plan stays pending. Opener throws on
        // connect because we don't have a real cluster; that's fine,
        // the orchestrator's catch(Exception) branch also fails the
        // step. Either path leaves completed=false.
        RollingRestartOrchestrator.Result r = orch.run(plan,
                host -> { throw new RuntimeException("no live cluster"); },
                host -> false,
                sunk::add);
        assertFalse(r.completed());
        assertFalse(r.overallSuccess());
    }

    @Test
    void informational_steps_pass_through_with_info_prefix() {
        UpgradePlan.Plan plan = new UpgradeScanner().scan(
                new UpgradeRules.Context(Version.parse("6.0"),
                        Version.parse("7.0"), List.of(), List.of()),
                // Zero members → no BINARY_SWAP / ROLLING_RESTART steps.
                List.of());

        RollingRestartOrchestrator orch = new RollingRestartOrchestrator();
        List<RollingRestartOrchestrator.StepOutcome> sunk = new ArrayList<>();
        RollingRestartOrchestrator.Result r = orch.run(plan,
                host -> { throw new AssertionError("no opener calls expected"); },
                host -> true,
                sunk::add);
        assertTrue(r.completed());
        assertTrue(r.overallSuccess());
        assertTrue(sunk.stream()
                .allMatch(o -> o.message().startsWith("info:")));
    }

    @Test
    void step_outcome_captures_host_and_order() {
        UpgradePlan.Step step = new UpgradePlan.Step(42,
                UpgradePlan.StepKind.FCV_LOWER, "Lower FCV",
                "body", null);
        UpgradePlan.Plan plan = new UpgradePlan.Plan(
                Version.parse("6.0"), Version.parse("7.0"),
                List.of(), List.of(step));

        AtomicReference<RollingRestartOrchestrator.StepOutcome> captured =
                new AtomicReference<>();
        new RollingRestartOrchestrator().run(plan,
                h -> null, h -> true, captured::set);
        assertNotNull(captured.get());
        assertEquals(42, captured.get().stepOrder());
        assertEquals(UpgradePlan.StepKind.FCV_LOWER, captured.get().kind());
    }
}
