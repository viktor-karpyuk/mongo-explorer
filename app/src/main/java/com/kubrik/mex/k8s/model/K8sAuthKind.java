package com.kubrik.mex.k8s.model;

/**
 * v2.8.1 — Classification of a kubeconfig user's authentication
 * strategy. Drives the auth-provider registry (technical-spec §3.3)
 * and the "plugin X not on PATH" pre-flight hint.
 *
 * <p>The official Java client handles every variant below internally;
 * this enum exists so the UI can say <i>"Signing in via aws-iam-
 * authenticator…"</i> rather than a generic "connecting…".</p>
 */
public enum K8sAuthKind {
    /** {@code user.exec} — e.g. aws-iam-authenticator, gke-gcloud-auth-plugin. */
    EXEC_PLUGIN,
    /** {@code user.auth-provider: oidc} — refresh-token flow. */
    OIDC,
    /** Static bearer token. */
    TOKEN,
    /** {@code client-certificate-data} or path. */
    CLIENT_CERT,
    /** Username / password basic auth (legacy clusters only). */
    BASIC_AUTH,
    /** In-cluster ServiceAccount token — only set when we boot inside a pod. */
    IN_CLUSTER,
    /** Kubeconfig parsed but no recognisable user stanza. Error surface. */
    UNKNOWN;
}
