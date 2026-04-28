package com.kubrik.mex.k8s.teardown;

import java.util.Objects;

/**
 * v2.8.1 Q2.8.1-I — Three explicit knobs the user sets before Delete.
 *
 * <p>No silent defaults (milestone §2.9, decision §7.10). Prod:
 * {@code deletePvcs=false}, {@code deleteSecrets=false}. Dev/Test:
 * both default true. User always confirms the combination explicitly.</p>
 */
public record CascadePlan(boolean deleteCr,
                            boolean deleteSecrets,
                            boolean deletePvcs) {

    public CascadePlan {
        Objects.requireNonNull(Boolean.valueOf(deleteCr));
    }

    public static CascadePlan prodDefaults() {
        // Prod keeps data — only the CR (which the operator
        // then garbage-collects owned objects for, except PVCs
        // which the user keeps explicitly).
        return new CascadePlan(true, false, false);
    }

    public static CascadePlan devDefaults() {
        return new CascadePlan(true, true, true);
    }

    public String summary() {
        StringBuilder sb = new StringBuilder();
        if (deleteCr) sb.append("delete CR");
        if (deleteSecrets) sb.append(sb.isEmpty() ? "" : ", ").append("delete Secrets");
        if (deletePvcs) sb.append(sb.isEmpty() ? "" : ", ").append("delete PVCs");
        if (sb.isEmpty()) sb.append("no-op");
        return sb.toString();
    }
}
