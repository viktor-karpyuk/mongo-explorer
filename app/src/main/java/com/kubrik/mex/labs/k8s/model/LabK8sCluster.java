package com.kubrik.mex.labs.k8s.model;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8-N1 — Row projection of {@code lab_k8s_clusters}.
 *
 * <p>Holds the distro-level identity the CLI tool needs
 * ({@link #identifier}) plus the pointers back to the production
 * k8s pipeline's {@link com.kubrik.mex.k8s.model.K8sClusterRef}
 * (via {@link #k8sClusterId}). A destroyed Lab keeps its row as a
 * tombstone with {@code status=DESTROYED}; the row is only deleted
 * when the user manually purges history.</p>
 */
public record LabK8sCluster(
        long id,
        LabK8sDistro distro,
        String identifier,
        String contextName,
        String kubeconfigPath,
        LabK8sClusterStatus status,
        long createdAt,
        Optional<Long> lastStartedAt,
        Optional<Long> lastStoppedAt,
        Optional<Long> destroyedAt,
        Optional<Long> k8sClusterId) {

    public LabK8sCluster {
        Objects.requireNonNull(distro, "distro");
        Objects.requireNonNull(identifier, "identifier");
        Objects.requireNonNull(contextName, "contextName");
        Objects.requireNonNull(kubeconfigPath, "kubeconfigPath");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(lastStartedAt, "lastStartedAt");
        Objects.requireNonNull(lastStoppedAt, "lastStoppedAt");
        Objects.requireNonNull(destroyedAt, "destroyedAt");
        Objects.requireNonNull(k8sClusterId, "k8sClusterId");
    }

    public String coordinates() {
        return distro.cliName() + ":" + identifier;
    }
}
