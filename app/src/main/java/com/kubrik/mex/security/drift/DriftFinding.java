package com.kubrik.mex.security.drift;

import java.util.Objects;

/**
 * v2.6 Q2.6-D1 — a single drift finding produced by the diff engine.
 *
 * @param path     dot-path inside the baseline payload — e.g.,
 *                 {@code users.dba@admin.roleBindings[0].role},
 *                 {@code roles.appOps@admin.inheritedPrivileges[2].actions}.
 *                 Stable across baseline captures so the {@code
 *                 sec_drift_acks} row can reference it long-term.
 * @param kind     ADDED / REMOVED / CHANGED.
 * @param before   JSON-ish stringified value from the baseline
 *                 ({@code null} when kind is ADDED).
 * @param after    JSON-ish stringified value from the current capture
 *                 ({@code null} when kind is REMOVED).
 * @param section  top-level subsystem — {@code users} or {@code roles};
 *                 used by the drift pane to group findings.
 */
public record DriftFinding(
        String path,
        Kind kind,
        String before,
        String after,
        String section
) {

    public enum Kind { ADDED, REMOVED, CHANGED }

    public DriftFinding {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(section, "section");
    }
}
