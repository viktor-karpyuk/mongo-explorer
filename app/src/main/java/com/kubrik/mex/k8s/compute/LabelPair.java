package com.kubrik.mex.k8s.compute;

import java.util.Objects;

/**
 * v2.8.2 Q2.8.2-A — Kubernetes label key/value pair.
 *
 * <p>Kept in the compute package (not the k8s.model package) so the
 * strategy model and renderer can share a single type without
 * dragging in the rest of the provisioning model tree. The wizard's
 * node-pool selector is a list of these; AND semantics.</p>
 */
public record LabelPair(String key, String value) {
    public LabelPair {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        if (key.isBlank()) throw new IllegalArgumentException("label key must not be blank");
    }
}
