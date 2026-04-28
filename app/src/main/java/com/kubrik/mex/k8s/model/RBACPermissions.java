package com.kubrik.mex.k8s.model;

import java.util.Objects;

/**
 * v2.8.1 — Boolean permission vector covering the RBAC facts the
 * pre-flight engine and UI need. The source of truth is a batch of
 * {@code SelfSubjectAccessReview} calls issued by {@link
 * com.kubrik.mex.k8s.cluster.RBACProbeService}.
 *
 * <p>{@code namespace} records the namespace the checks were scoped
 * to; {@code canListNamespaces} + {@code canCreateNamespace} are
 * cluster-scoped flags that don't care about it. Pre-flight reports
 * {@code !canCreateMcoCr && !canCreatePsmdbCr} as a single unified
 * "no operator CRs can be created" fail.</p>
 */
public record RBACPermissions(
        String namespace,
        boolean canListPods,
        boolean canReadEvents,
        boolean canReadSecrets,
        boolean canCreateSecrets,
        boolean canCreatePvcs,
        boolean canCreateMcoCr,
        boolean canCreatePsmdbCr,
        boolean canListNamespaces,
        boolean canCreateNamespace) {

    public RBACPermissions {
        Objects.requireNonNull(namespace, "namespace");
    }

    /** True when everything the provisioning path needs in {@code namespace} is allowed. */
    public boolean canProvision() {
        return canListPods && canReadEvents
                && canReadSecrets && canCreateSecrets
                && canCreatePvcs
                && (canCreateMcoCr || canCreatePsmdbCr);
    }
}
