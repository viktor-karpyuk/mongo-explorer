package com.kubrik.mex.k8s.model;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 — Row projection of {@code k8s_clusters}.
 *
 * <p>A cluster ref is the <em>coordinate</em> of a Kubernetes context the
 * user has attached to Mongo Explorer: which kubeconfig file + which
 * context within it. We never copy kubeconfig bytes into our store;
 * the ref points at the on-disk file and we re-read it on every
 * client build. That way users can {@code kubectl config set-context}
 * or rotate certs without re-registering the cluster here.</p>
 */
public record K8sClusterRef(
        long id,
        String displayName,
        String kubeconfigPath,
        String contextName,
        Optional<String> defaultNamespace,
        Optional<String> serverUrl,
        long addedAt,
        Optional<Long> lastUsedAt) {

    public K8sClusterRef {
        Objects.requireNonNull(displayName, "displayName");
        Objects.requireNonNull(kubeconfigPath, "kubeconfigPath");
        Objects.requireNonNull(contextName, "contextName");
        Objects.requireNonNull(defaultNamespace, "defaultNamespace");
        Objects.requireNonNull(serverUrl, "serverUrl");
        Objects.requireNonNull(lastUsedAt, "lastUsedAt");
    }

    /** Cheap identity string for logs + event-bus routing. */
    public String coordinates() {
        return contextName + "@" + kubeconfigPath;
    }
}
