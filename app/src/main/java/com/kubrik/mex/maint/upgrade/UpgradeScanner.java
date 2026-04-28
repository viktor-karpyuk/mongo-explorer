package com.kubrik.mex.maint.upgrade;

import com.kubrik.mex.maint.model.UpgradePlan;
import com.kubrik.mex.maint.model.UpgradePlan.Step;
import com.kubrik.mex.maint.model.UpgradePlan.StepKind;
import com.kubrik.mex.maint.model.UpgradePlan.Version;

import java.util.ArrayList;
import java.util.List;

/**
 * v2.7 UPG-1/2/3 — Orchestrates an upgrade scan: feeds the
 * {@link UpgradeRules} a context, builds the ordered step list the
 * runbook renderer later turns into Markdown + HTML.
 */
public final class UpgradeScanner {

    private final UpgradeRules rules;

    public UpgradeScanner() { this(new UpgradeRules()); }

    public UpgradeScanner(UpgradeRules rules) { this.rules = rules; }

    public UpgradePlan.Plan scan(UpgradeRules.Context ctx,
                                 List<String> memberHosts) {
        List<UpgradePlan.Finding> findings = rules.evaluate(ctx);
        List<Step> steps = buildSteps(ctx, memberHosts);
        return new UpgradePlan.Plan(ctx.from(), ctx.to(), findings, steps);
    }

    private List<Step> buildSteps(UpgradeRules.Context ctx,
                                  List<String> memberHosts) {
        List<Step> out = new ArrayList<>();
        int order = 1;
        out.add(new Step(order++, StepKind.PRE_CHECK,
                "Pre-flight cluster state",
                "Verify cluster health, backup freshness, oplog "
                        + "window, and disk free space on every member.",
                null));
        out.add(new Step(order++, StepKind.BACKUP,
                "Take a fresh backup",
                "Confirm a current backup exists and has passed "
                        + "verification. Upgrades are most recoverable "
                        + "when the last backup is < 24 h old.",
                null));
        out.add(new Step(order++, StepKind.FCV_LOWER,
                "Set featureCompatibilityVersion to " + ctx.from().asMajorMinor(),
                "db.adminCommand({ setFeatureCompatibilityVersion: \""
                        + ctx.from().asMajorMinor()
                        + "\", confirm: true })",
                null));
        // Rolling restart — secondaries first, primary last.
        int secCount = Math.max(0, memberHosts.size() - 1);
        for (int i = 0; i < secCount; i++) {
            String h = memberHosts.get(i);
            out.add(new Step(order++, StepKind.BINARY_SWAP,
                    "Swap binary on secondary " + h,
                    "Stop mongod, install the " + ctx.to().asMajorMinor()
                            + " binary, start mongod, wait for "
                            + "stateStr=SECONDARY before continuing.",
                    h));
        }
        // Step-down before primary swap.
        if (!memberHosts.isEmpty()) {
            String primary = memberHosts.get(memberHosts.size() - 1);
            out.add(new Step(order++, StepKind.ROLLING_RESTART,
                    "Step down the primary",
                    "db.adminCommand({ replSetStepDown: 60 }) on "
                            + primary + ". Wait for the election to "
                            + "complete.",
                    primary));
            out.add(new Step(order++, StepKind.BINARY_SWAP,
                    "Swap binary on former primary " + primary,
                    "Now a secondary; apply the same binary swap as "
                            + "the other nodes and wait for "
                            + "stateStr=SECONDARY.",
                    primary));
        }
        out.add(new Step(order++, StepKind.FCV_RAISE,
                "Raise featureCompatibilityVersion to " + ctx.to().asMajorMinor(),
                "Only after every member is running the new binary:\n"
                        + "db.adminCommand({ "
                        + "setFeatureCompatibilityVersion: \""
                        + ctx.to().asMajorMinor() + "\", confirm: true })",
                null));
        out.add(new Step(order++, StepKind.POST_CHECK,
                "Post-upgrade verification",
                "Confirm cluster health, re-run monitoring dashboards, "
                        + "and verify the application smoke-test.",
                null));
        return out;
    }
}
