package com.kubrik.mex.k8s.preflight;

import com.kubrik.mex.k8s.provision.ProvisionModel;
import io.kubernetes.client.openapi.ApiClient;

/**
 * v2.8.1 Q2.8.1-G — One pre-flight check.
 *
 * <p>Implementations are pure functions of {@code (client, model)};
 * they may talk to the Kubernetes API but should not mutate cluster
 * state. Each check runs on the {@link PreflightEngine}'s bounded
 * pool so a slow one can't stall Apply.</p>
 */
public interface PreflightCheck {

    /** Machine-friendly id — e.g. {@code "preflight.storage-class"}. */
    String id();

    /** Human-friendly label shown next to the check's result chip. */
    String label();

    /** Does this check apply to the given model? */
    PreflightScope scope(ProvisionModel model);

    /** Execute the check. Returns PASS / WARN / FAIL + optional hint. */
    PreflightResult run(ApiClient client, ProvisionModel model);

    enum PreflightScope {
        /** Runs on every provision — e.g. RBAC probe. */
        ALWAYS,
        /** Runs only when the model has some relevant property — engine calls {@link #scope} first. */
        CONDITIONAL,
        /** Don't run — engine records it as {@link PreflightResult#skipped}. */
        SKIP
    }
}
