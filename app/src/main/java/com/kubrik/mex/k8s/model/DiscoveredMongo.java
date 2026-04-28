package com.kubrik.mex.k8s.model;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-B — Unified projection of a Mongo workload the user
 * can pick from the Clusters pane and "connect to existing."
 *
 * <p>Origin-agnostic: rows can come from any of the three
 * discoverers (MCO CR, PSMDB CR, plain StatefulSet). The {@link
 * Origin} enum + {@code operatorResourceName} / {@code statefulSetName}
 * pair capture which source produced the row; the wizard's Clone
 * flow downstream uses this to re-render the right CR shape.</p>
 *
 * <p>{@code ready} is a best-effort aggregate — for operator CRs we
 * read the status subresource; for plain STS we read
 * {@code readyReplicas == replicas}. {@code null} here means "the
 * discoverer was reachable but couldn't make a determination" and
 * the UI renders a question-mark chip.</p>
 */
public record DiscoveredMongo(
        long clusterId,
        Origin origin,
        String namespace,
        String name,
        Topology topology,
        Optional<String> serviceName,
        Optional<Integer> port,
        AuthKind authKind,
        Optional<Boolean> ready,
        Optional<String> mongoVersion,
        Optional<String> crResourceName,
        Optional<String> statefulSetName) {

    public enum Origin {
        MCO,
        PSMDB,
        PLAIN_STS
    }

    public enum Topology {
        STANDALONE,
        RS3,
        RS5,
        SHARDED,
        UNKNOWN
    }

    public enum AuthKind {
        NONE,
        SCRAM,
        X509,
        AWS_IAM,
        UNKNOWN
    }

    public DiscoveredMongo {
        Objects.requireNonNull(origin, "origin");
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(topology, "topology");
        Objects.requireNonNull(serviceName, "serviceName");
        Objects.requireNonNull(port, "port");
        Objects.requireNonNull(authKind, "authKind");
        Objects.requireNonNull(ready, "ready");
        Objects.requireNonNull(mongoVersion, "mongoVersion");
        Objects.requireNonNull(crResourceName, "crResourceName");
        Objects.requireNonNull(statefulSetName, "statefulSetName");
    }

    /** Stable-ish identity string for logs + event routing. */
    public String coordinates() {
        return origin.name().toLowerCase() + ":" + namespace + "/" + name;
    }
}
