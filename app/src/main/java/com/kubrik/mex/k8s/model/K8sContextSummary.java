package com.kubrik.mex.k8s.model;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 — One row of the kubeconfig context picker.
 *
 * <p>Produced by {@link com.kubrik.mex.k8s.client.KubeConfigLoader}.
 * The {@code authKind} + {@code authDetail} pair lets the "Add
 * cluster" dialog render a specific hint next to each context
 * (e.g. {@code "exec: aws-iam-authenticator token"}).</p>
 *
 * <p>{@code execBinary} is the resolvable name / path the loader
 * extracted from a detected exec plugin. It is informational — the
 * actual run happens inside the Kubernetes Java client. We surface
 * it so the pre-flight UI can warn "{@code aws-iam-authenticator}
 * not on PATH" before the user hits Apply.</p>
 */
public record K8sContextSummary(
        String contextName,
        String clusterName,
        String userName,
        Optional<String> namespace,
        Optional<String> serverUrl,
        K8sAuthKind authKind,
        Optional<String> authDetail,
        Optional<String> execBinary) {

    public K8sContextSummary {
        Objects.requireNonNull(contextName, "contextName");
        Objects.requireNonNull(clusterName, "clusterName");
        Objects.requireNonNull(userName, "userName");
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(serverUrl, "serverUrl");
        Objects.requireNonNull(authKind, "authKind");
        Objects.requireNonNull(authDetail, "authDetail");
        Objects.requireNonNull(execBinary, "execBinary");
    }
}
