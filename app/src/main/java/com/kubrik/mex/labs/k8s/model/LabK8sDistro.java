package com.kubrik.mex.labs.k8s.model;

/**
 * v2.8.1 Q2.8-N1 — Kubernetes distribution the local Lab runs on.
 *
 * <p>Strictly two choices (milestone decision 1, {@code NG-LAB-K8S-2}):
 * {@code minikube} and {@code k3d}. Bare k3s is a permanent
 * non-goal — no install path, no detection heuristic, no
 * compatibility shim.</p>
 */
public enum LabK8sDistro {
    MINIKUBE,
    K3D;

    public String cliName() {
        return switch (this) {
            case MINIKUBE -> "minikube";
            case K3D -> "k3d";
        };
    }

    /** kubeconfig context name convention per distro after start. */
    public String contextFor(String identifier) {
        return switch (this) {
            case MINIKUBE -> identifier;                // minikube uses profile name as context
            case K3D -> "k3d-" + identifier;            // k3d prefixes
        };
    }
}
