package com.kubrik.mex.backup.spec;

import com.kubrik.mex.migration.schedule.CronExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * v2.5 BKP-POLICY-1..7 — pre-persist validator for {@link BackupPolicy}.
 * Returns a list of human-readable error strings; callers render them on
 * the policy editor.
 *
 * <p>Split from the record's canonical constructor so the editor can show
 * every problem at once rather than surfacing them one-by-one as the user
 * types. The record constructors still enforce structural invariants (the
 * kind of mistakes only a buggy caller could make).</p>
 */
public final class PolicyValidator {

    static final Pattern NAME_PATTERN = Pattern.compile("[\\w .-]+");

    private PolicyValidator() {}

    public static List<String> validate(BackupPolicy p) {
        List<String> errors = new ArrayList<>();

        // BKP-POLICY-2 — name rules.
        String name = p.name() == null ? "" : p.name().trim();
        if (name.isEmpty()) errors.add("name is required");
        if (name.length() > 64) errors.add("name must be 64 characters or fewer");
        if (!name.isEmpty() && !NAME_PATTERN.matcher(name).matches())
            errors.add("name may only contain letters, digits, spaces, dots, dashes, and underscores");

        // BKP-POLICY-3 — cron (null means manual only).
        if (p.scheduleCron() != null && !p.scheduleCron().isBlank()) {
            try { CronExpression.parse(p.scheduleCron()); }
            catch (IllegalArgumentException bad) {
                errors.add("invalid cron expression: " + bad.getMessage());
            }
        }

        // BKP-POLICY-4 — scope is non-null (enforced by the record); add a
        // helpful empty-list message if someone synthesises an invalid list
        // via reflection / deserialisation.
        if (p.scope() instanceof Scope.Databases d && d.names().isEmpty())
            errors.add("databases scope cannot be empty");
        if (p.scope() instanceof Scope.Namespaces ns && ns.namespaces().isEmpty())
            errors.add("namespaces scope cannot be empty");

        // BKP-POLICY-5 — retention bounds (record enforces, repeat check for
        // deserialisation paths that might bypass the canonical ctor).
        if (p.retention() != null) {
            int mc = p.retention().maxCount();
            int ma = p.retention().maxAgeDays();
            if (mc < 1 || mc > 1000)
                errors.add("retention.maxCount must be 1..1000");
            if (ma < 1 || ma > 3650)
                errors.add("retention.maxAgeDays must be 1..3650");
        }

        // BKP-POLICY-6 — archive level bound when gzip is on.
        if (p.archive() != null && p.archive().gzip()) {
            int lvl = p.archive().level();
            if (lvl < 1 || lvl > 9)
                errors.add("archive.level must be 1..9 when gzip is enabled");
        }

        // BKP-POLICY-7 — includeOplog is a boolean; no explicit failure mode
        // here beyond a runner-side warning when the source isn't a replset.

        if (p.sinkId() <= 0) errors.add("sinkId must reference an existing storage_sinks row");

        return errors;
    }

    public static boolean isValid(BackupPolicy p) { return validate(p).isEmpty(); }
}
