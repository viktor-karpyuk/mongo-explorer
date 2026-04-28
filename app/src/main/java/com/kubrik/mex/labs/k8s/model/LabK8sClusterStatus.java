package com.kubrik.mex.labs.k8s.model;

/**
 * v2.8.1 Q2.8-N1 — Lifecycle status of a local Lab K8s cluster.
 *
 * <p>Mirrors {@code labs.model.LabStatus} — separate enum because
 * distro clusters have one more state (AUTH_EXPIRED is a k8s-only
 * concept we may add later) and SQLite stores the value as text.</p>
 */
public enum LabK8sClusterStatus {
    CREATING,
    RUNNING,
    STOPPED,
    FAILED,
    DESTROYED
}
