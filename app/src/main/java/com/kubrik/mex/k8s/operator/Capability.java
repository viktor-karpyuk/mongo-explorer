package com.kubrik.mex.k8s.operator;

/**
 * v2.8.1 Q2.8.1-E1 — Per-operator feature flags.
 *
 * <p>The wizard queries {@link OperatorAdapter#capabilities()} to
 * decide which steps to show. MCO and PSMDB diverge on two axes
 * that matter most:</p>
 *
 * <ul>
 *   <li>{@link #SHARDED} — PSMDB only; the wizard hides the
 *       sharded radio when MCO is selected (explain inline).</li>
 *   <li>{@link #NATIVE_BACKUP} — PSMDB ships PBM. MCO does not, so
 *       the Backup step offers the Managed-CronJob or BYO-Declared
 *       path instead of a "backup off" option in Prod.</li>
 * </ul>
 *
 * <p>Remaining flags are less load-bearing but still wired so the
 * preview pane can explain what got rendered and what didn't.</p>
 */
public enum Capability {
    /** Sharded clusters (cfg replset + shard replsets + mongos). */
    SHARDED,
    /** Operator ships a built-in backup solution (PSMDB → PBM). */
    NATIVE_BACKUP,
    /** Operator can generate its own TLS material end-to-end (MCO + PSMDB both support this). */
    OPERATOR_GENERATED_TLS,
    /** Operator honours a cert-manager Issuer reference for TLS. */
    CERT_MANAGER_TLS,
    /** Operator supports in-place mongod version upgrades. */
    IN_PLACE_UPGRADE,
    /** Operator emits a ServiceMonitor ready CR field (vs. a separate manifest). */
    NATIVE_SERVICE_MONITOR,
    /** Operator can inject arbiter-only members. */
    ARBITER_MEMBERS
}
