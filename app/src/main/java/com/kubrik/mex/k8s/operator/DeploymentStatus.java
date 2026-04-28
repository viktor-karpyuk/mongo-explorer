package com.kubrik.mex.k8s.operator;

/**
 * v2.8.1 Q2.8.1-E1 — Unified status a per-operator status parser
 * returns to the rollout viewer.
 *
 * <p>Both MCO and PSMDB expose an operator-specific {@code
 * status.phase} string; the parser flattens those into this enum so
 * the UI chip + rollout viewer don't have to carry operator-
 * awareness past the adapter boundary.</p>
 */
public enum DeploymentStatus {
    /** Operator accepted the CR; resources being created. */
    APPLYING,
    /** Mongo is up and primary elected. */
    READY,
    /** CR rejected or a pod stuck in CrashLoopBackOff past the budget. */
    FAILED,
    /** Adapter couldn't make a determination yet — UI renders a spinner. */
    UNKNOWN
}
