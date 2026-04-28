package com.kubrik.mex.maint.upgrade;

import com.kubrik.mex.maint.model.UpgradePlan;
import com.kubrik.mex.maint.model.UpgradePlan.Finding;
import com.kubrik.mex.maint.model.UpgradePlan.Severity;
import com.kubrik.mex.maint.model.UpgradePlan.Version;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * v2.7 UPG-* — Versioned rule pack. Each rule receives the source
 * + target version and optional sampled profile / config data, and
 * returns zero or more {@link Finding}s.
 *
 * <p>The rules that ship in v2.7 cover the 4.4 → 5.0 → 6.0 → 7.0
 * jumps — the three most common upgrade paths operators face. Users
 * can extend via a local overrides file (future polish).</p>
 */
public final class UpgradeRules {

    public record Context(
            Version from,
            Version to,
            List<String> operatorsSeenInProfile,
            List<String> parametersSetOnServer
    ) {
        public Context {
            operatorsSeenInProfile = List.copyOf(operatorsSeenInProfile);
            parametersSetOnServer = List.copyOf(parametersSetOnServer);
        }
    }

    /** Evaluate every rule in order; returns the accumulated list. */
    public List<Finding> evaluate(Context ctx) {
        List<Finding> out = new ArrayList<>();
        for (BiFunction<Context, List<Finding>, Void> rule : RULES) {
            rule.apply(ctx, out);
        }
        return List.copyOf(out);
    }

    /* =========================== rule registry =========================== */

    private static final List<BiFunction<Context, List<Finding>, Void>> RULES = List.of(
            UpgradeRules::versionGapRule,
            UpgradeRules::deprecatedOperatorsRule,
            UpgradeRules::removedParametersRule,
            UpgradeRules::fcvStepRule
    );

    /** Only the immediate-neighbour major hops are supported. Jumping
     *  4.4 → 7.0 directly would require multiple FCV steps the runner
     *  can't represent safely in a single plan. */
    private static Void versionGapRule(Context ctx, List<Finding> out) {
        int gap = ctx.to().major() - ctx.from().major();
        if (gap > 1) {
            out.add(new Finding(UpgradePlan.FindingKind.VERSION_GAP,
                    "VERSION-GAP-TOO-WIDE", Severity.BLOCK,
                    "Upgrade gap > 1 major version",
                    "Mongo does not support skipping major versions. "
                            + "Upgrade to " + (ctx.from().major() + 1)
                            + ".x first, complete the FCV step, then "
                            + "proceed to " + ctx.to().asMajorMinor() + ".",
                    "Split the upgrade into " + gap + " passes."));
        } else if (gap < 0) {
            out.add(new Finding(UpgradePlan.FindingKind.VERSION_GAP,
                    "DOWNGRADE", Severity.BLOCK,
                    "Target version is older than source",
                    "Mongo Explorer does not support downgrades via the "
                            + "upgrade planner. Restore from backup and "
                            + "apply a PITR replay if a downgrade is truly "
                            + "needed.",
                    "Abort. Use backups + restore for a downgrade."));
        }
        return null;
    }

    /** Operators that landed as deprecated-and-removed in the target. */
    private static Void deprecatedOperatorsRule(Context ctx, List<Finding> out) {
        List<String> removedInTarget = operatorsRemovedAt(ctx.to());
        for (String op : ctx.operatorsSeenInProfile()) {
            if (removedInTarget.contains(op)) {
                out.add(new Finding(UpgradePlan.FindingKind.OP_DEPRECATED,
                        "OP-REMOVED-" + op, Severity.WARN,
                        "Query operator '" + op + "' removed in "
                                + ctx.to().asMajorMinor(),
                        "Your profile data shows active use of '" + op
                                + "' — queries using it will fail after "
                                + "the upgrade.",
                        "Rewrite query plans using '" + op + "'."));
            }
        }
        return null;
    }

    /** {@code setParameter} entries removed or gated in the target. */
    private static Void removedParametersRule(Context ctx, List<Finding> out) {
        List<String> removedInTarget = parametersRemovedAt(ctx.to());
        for (String p : ctx.parametersSetOnServer()) {
            if (removedInTarget.contains(p)) {
                out.add(new Finding(UpgradePlan.FindingKind.PARAM_REMOVED,
                        "PARAM-REMOVED-" + p, Severity.WARN,
                        "Parameter '" + p + "' removed in "
                                + ctx.to().asMajorMinor(),
                        "The server currently has '" + p + "' set; the "
                                + "target version rejects this name on "
                                + "setParameter.",
                        "Remove '" + p + "' from startup config or config "
                                + "management."));
            }
        }
        return null;
    }

    /** FCV must be at the current version before moving forward; the
     *  runbook includes a lower-FCV step if it's out of sync. */
    private static Void fcvStepRule(Context ctx, List<Finding> out) {
        out.add(new Finding(UpgradePlan.FindingKind.FCV_STEP,
                "FCV-REQUIRED", Severity.INFO,
                "FCV must match source before binary swap",
                "Before swapping binaries, set "
                        + "featureCompatibilityVersion to "
                        + ctx.from().asMajorMinor()
                        + ". The runbook includes this step.",
                "The runbook emits an FCV_LOWER step."));
        out.add(new Finding(UpgradePlan.FindingKind.FCV_STEP,
                "FCV-RAISE", Severity.INFO,
                "FCV must be raised post-upgrade",
                "Once all members are running " + ctx.to().asMajorMinor()
                        + ", raise featureCompatibilityVersion to "
                        + ctx.to().asMajorMinor()
                        + " to enable new features.",
                "The runbook emits an FCV_RAISE step."));
        return null;
    }

    /* ============================= fact tables ============================= */

    /** Operators that were removed at the given major version (best-
     *  effort snapshot per MongoDB release notes). Extendable via the
     *  future overrides file. */
    private static List<String> operatorsRemovedAt(Version v) {
        if (v.major() == 5) return List.of("$geoNear" /* legacy form */);
        if (v.major() == 6) return List.of();
        if (v.major() == 7) return List.of("$currentDate-timestamp-variant"
                /* synthetic placeholder until the rule pack is fleshed */);
        return List.of();
    }

    /** Parameters removed at the given major version. */
    private static List<String> parametersRemovedAt(Version v) {
        if (v.major() == 5) return List.of("failIndexKeyTooLong");
        if (v.major() == 6) return List.of();
        if (v.major() == 7) return List.of();
        return List.of();
    }

    public UpgradeRules() {}
}
