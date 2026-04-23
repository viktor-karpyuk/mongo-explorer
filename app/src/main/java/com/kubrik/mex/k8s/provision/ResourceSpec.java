package com.kubrik.mex.k8s.provision;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-D1 — CPU / memory requests + limits per member kind.
 *
 * <p>Values are in Kubernetes resource-string form
 * ({@code "500m"}, {@code "1Gi"}). Prod enforces non-empty on the
 * data-pod fields; mongos (sharded only) defaults to a small
 * fixed budget.</p>
 *
 * <p>An empty request/limit string means "inherit operator default"
 * — which varies per operator. The wizard renders this as "use
 * operator default" so users don't think it's an error.</p>
 */
public record ResourceSpec(
        Optional<String> dataCpuRequest,
        Optional<String> dataMemRequest,
        Optional<String> dataCpuLimit,
        Optional<String> dataMemLimit,
        Optional<String> mongosCpuRequest,
        Optional<String> mongosMemRequest) {

    public ResourceSpec {
        Objects.requireNonNull(dataCpuRequest, "dataCpuRequest");
        Objects.requireNonNull(dataMemRequest, "dataMemRequest");
        Objects.requireNonNull(dataCpuLimit, "dataCpuLimit");
        Objects.requireNonNull(dataMemLimit, "dataMemLimit");
        Objects.requireNonNull(mongosCpuRequest, "mongosCpuRequest");
        Objects.requireNonNull(mongosMemRequest, "mongosMemRequest");
    }

    public static ResourceSpec devDefaults() {
        return new ResourceSpec(
                Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
    }

    /** Prod defaults are small-but-non-empty so the Prod contract stays honest. */
    public static ResourceSpec prodDefaults() {
        return new ResourceSpec(
                Optional.of("500m"), Optional.of("1Gi"),
                Optional.of("2"), Optional.of("4Gi"),
                Optional.of("200m"), Optional.of("256Mi"));
    }

    public boolean hasDataRequests() {
        return dataCpuRequest.isPresent() && dataMemRequest.isPresent();
    }
}
