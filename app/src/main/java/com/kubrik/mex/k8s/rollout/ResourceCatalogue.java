package com.kubrik.mex.k8s.rollout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * v2.8.1 Q2.8.1-H — Tracks every Kubernetes object the apply
 * orchestrator created, so the cleanup path can tear them down in
 * reverse order.
 *
 * <p>Append-only during Apply; iterated in reverse on cleanup. Each
 * entry carries the {@code (apiVersion, kind, namespace, name)}
 * tuple the delete call needs — no heap-resident Kubernetes
 * objects here, just identifiers.</p>
 */
public final class ResourceCatalogue {

    public record Ref(String apiVersion, String kind, String namespace, String name) {
        public Ref {
            Objects.requireNonNull(apiVersion, "apiVersion");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(namespace, "namespace");
            Objects.requireNonNull(name, "name");
        }
    }

    private final List<Ref> refs = new ArrayList<>();

    public synchronized void record(Ref ref) {
        refs.add(ref);
    }

    public synchronized List<Ref> snapshot() {
        return Collections.unmodifiableList(new ArrayList<>(refs));
    }

    /** Snapshot in reverse order — what the cleanup iterator wants. */
    public synchronized List<Ref> reversed() {
        List<Ref> out = new ArrayList<>(refs);
        Collections.reverse(out);
        return Collections.unmodifiableList(out);
    }

    public synchronized int size() { return refs.size(); }
}
