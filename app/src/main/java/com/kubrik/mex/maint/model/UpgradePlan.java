package com.kubrik.mex.maint.model;

import java.util.List;
import java.util.Objects;

/**
 * v2.7 UPG-* — Output of {@code UpgradeScanner}: a series of
 * {@link Finding}s flagged against the current cluster + the
 * {@link Step}s a runbook should emit.
 */
public final class UpgradePlan {

    public enum FindingKind { OP_DEPRECATED, PARAM_REMOVED, FCV_STEP, VERSION_GAP }

    public enum Severity { INFO, WARN, BLOCK }

    public record Finding(
            FindingKind kind,
            String code,        // e.g. "OP-DEPRECATED-$where"
            Severity severity,
            String title,
            String detail,
            String remediation  // nullable
    ) {
        public Finding {
            Objects.requireNonNull(kind);
            Objects.requireNonNull(code);
            Objects.requireNonNull(severity);
            Objects.requireNonNull(title);
            Objects.requireNonNull(detail);
        }
    }

    public enum StepKind {
        PRE_CHECK, BACKUP, FCV_LOWER, BINARY_SWAP, ROLLING_RESTART,
        FCV_RAISE, POST_CHECK
    }

    public record Step(
            int order,
            StepKind kind,
            String title,
            String body,
            String targetHost     // nullable (cluster-wide step)
    ) {
        public Step {
            Objects.requireNonNull(kind);
            Objects.requireNonNull(title);
            Objects.requireNonNull(body);
        }
    }

    public record Version(int major, int minor, int patch) {
        public Version {
            if (major < 0 || minor < 0 || patch < 0)
                throw new IllegalArgumentException("version components must be ≥ 0");
        }
        public String asMajorMinor() { return major + "." + minor; }
        public String asFull() { return major + "." + minor + "." + patch; }
        public static Version parse(String s) {
            String[] parts = s.split("\\.");
            int mj = Integer.parseInt(parts[0]);
            int mn = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
            int p = parts.length > 2
                    ? Integer.parseInt(parts[2].replaceAll("[^0-9].*$", ""))
                    : 0;
            return new Version(mj, mn, p);
        }
    }

    public record Plan(
            Version from,
            Version to,
            List<Finding> findings,
            List<Step> steps
    ) {
        public Plan {
            Objects.requireNonNull(from);
            Objects.requireNonNull(to);
            findings = List.copyOf(findings);
            steps = List.copyOf(steps);
        }

        public boolean hasBlockers() {
            return findings.stream().anyMatch(f -> f.severity() == Severity.BLOCK);
        }
    }

    private UpgradePlan() {}
}
