package com.kubrik.mex.k8s.model;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-C1 — Coordinates of a port-forward target.
 *
 * <p>Callers pick one of two shapes:</p>
 * <ul>
 *   <li><b>Pod.</b> {@code pod()} is set; the forward goes straight to
 *       a named pod. Used by the provisioning flow once it knows the
 *       primary pod of the freshly-applied CR.</li>
 *   <li><b>Service.</b> {@code service()} is set; the port-forward
 *       service resolves the Service to a ready pod via Endpoints
 *       and forwards there. Used by "connect to existing" on
 *       {@code DiscoveredMongo}.</li>
 * </ul>
 *
 * <p>The two are mutually exclusive — {@link #validate()} enforces
 * that. A record with neither or both fails fast at construction so
 * the service layer can't accidentally reach past an empty target.</p>
 */
public record PortForwardTarget(
        String namespace,
        Optional<String> pod,
        Optional<String> service,
        int remotePort) {

    public PortForwardTarget {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(pod, "pod");
        Objects.requireNonNull(service, "service");
        if (namespace.isBlank()) {
            throw new IllegalArgumentException("namespace must not be blank");
        }
        if (remotePort <= 0 || remotePort > 65535) {
            throw new IllegalArgumentException("remotePort out of range: " + remotePort);
        }
        if (pod.isPresent() == service.isPresent()) {
            throw new IllegalArgumentException(
                    "exactly one of pod or service must be set");
        }
    }

    public static PortForwardTarget forService(String namespace, String serviceName, int port) {
        return new PortForwardTarget(namespace, Optional.empty(),
                Optional.of(serviceName), port);
    }

    public static PortForwardTarget forPod(String namespace, String podName, int port) {
        return new PortForwardTarget(namespace, Optional.of(podName),
                Optional.empty(), port);
    }

    /** {@code SERVICE} or {@code POD} — emitted into the audit row. */
    public String kindLabel() {
        return pod.isPresent() ? "POD" : "SERVICE";
    }

    public String name() {
        return pod.orElseGet(service::get);
    }

    public void validate() {
        // no-op; validation runs in the canonical constructor.
    }
}
