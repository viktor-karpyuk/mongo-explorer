package com.kubrik.mex.k8s.model;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-H — Row projection of {@code provisioning_records}.
 *
 * <p>Written by {@link com.kubrik.mex.k8s.apply.ApplyOrchestrator}
 * at Apply start in state {@code APPLYING}; updated through the
 * rollout to {@code READY} / {@code FAILED} / {@code DELETED}.</p>
 */
public record ProvisioningRecord(
        long id,
        long k8sClusterId,
        String namespace,
        String name,
        String operator,
        String operatorVersion,
        String mongoVersion,
        String topology,
        String profile,
        String crYaml,
        String crSha256,
        boolean deletionProtection,
        long createdAt,
        Optional<Long> appliedAt,
        Status status,
        Optional<String> connectionId) {

    public enum Status { APPLYING, READY, FAILED, DELETING, DELETED }

    public ProvisioningRecord {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(operator, "operator");
        Objects.requireNonNull(operatorVersion, "operatorVersion");
        Objects.requireNonNull(mongoVersion, "mongoVersion");
        Objects.requireNonNull(topology, "topology");
        Objects.requireNonNull(profile, "profile");
        Objects.requireNonNull(crYaml, "crYaml");
        Objects.requireNonNull(crSha256, "crSha256");
        Objects.requireNonNull(appliedAt, "appliedAt");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(connectionId, "connectionId");
    }
}
