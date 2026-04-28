package com.kubrik.mex.k8s.operator;

import java.util.List;
import java.util.Objects;

/**
 * v2.8.1 Q2.8.1-E1 — The set of rendered YAML documents an
 * {@link OperatorAdapter} produces for a single deployment.
 *
 * <p>Strictly ordered: the apply orchestrator (Q2.8.1-H) walks
 * {@link #documents()} front-to-back so Secrets + ConfigMaps land
 * before the CR references them. Every document carries its
 * {@code apiVersion / kind / metadata.name} triple in {@link
 * Manifest#kind} + {@link Manifest#name} for the dry-run UI's
 * summary row.</p>
 *
 * <p>{@link #crYaml()} is a convenience accessor for the CR document
 * the adapter always emits — the preview pane renders it
 * prominently as the "main" manifest.</p>
 */
public record KubernetesManifests(
        String crKind,
        String crName,
        String crYaml,
        List<Manifest> documents) {

    public KubernetesManifests {
        Objects.requireNonNull(crKind, "crKind");
        Objects.requireNonNull(crName, "crName");
        Objects.requireNonNull(crYaml, "crYaml");
        Objects.requireNonNull(documents, "documents");
        documents = List.copyOf(documents);
    }

    /**
     * Single rendered document.
     *
     * @param kind Kubernetes Kind (e.g. {@code Secret}, {@code PodDisruptionBudget}).
     * @param name {@code metadata.name} of the document.
     * @param yaml Full YAML body, including {@code apiVersion} + {@code kind}.
     */
    public record Manifest(String kind, String name, String yaml) {
        public Manifest {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(yaml, "yaml");
        }
    }

    /** Convenience — total document count for the preview pane. */
    public int size() { return documents.size(); }
}
