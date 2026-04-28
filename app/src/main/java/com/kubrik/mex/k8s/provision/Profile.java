package com.kubrik.mex.k8s.provision;

/**
 * v2.8.1 Q2.8.1-D1 — Deployment profile.
 *
 * <p>Two choices only. There is no "Custom / Advanced" tier in
 * v2.8.1 Alpha (milestone §7.2 decision, open question 9.2 —
 * resolved for v2.8.1 as "defer"). The discriminator flows through
 * the wizard's field verdicts; {@link ProfileEnforcer} is the
 * single source of truth for what each value enforces.</p>
 */
public enum Profile {
    /** Flexible preset. Sensible defaults; every knob overridable. */
    DEV_TEST,
    /** Strict mode. Prod-required settings enforced; disabling them requires switching profile. */
    PROD;
}
