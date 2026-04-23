package com.kubrik.mex.labs.model;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.4 — Row view of {@code lab_deployments}. One per Lab a user
 * has created; the table is write-once-then-mutated (status + timing
 * fields change across the lifecycle, the immutable provenance
 * fields — template id, compose project, compose file — do not).
 */
public record LabDeployment(
        long id,
        String templateId,
        String templateVersion,      // which Mongo Explorer release rendered it
        String displayName,
        String composeProject,        // mex-lab-<template>-<uuid8>
        String composeFilePath,
        PortMap portMap,
        LabStatus status,
        boolean keepDataOnStop,
        boolean authEnabled,
        long createdAt,
        Optional<Long> lastStartedAt,
        Optional<Long> lastStoppedAt,
        Optional<Long> destroyedAt,
        String mongoImageTag,
        Optional<String> connectionId
) {
    public LabDeployment {
        Objects.requireNonNull(templateId, "templateId");
        Objects.requireNonNull(composeProject, "composeProject");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(portMap, "portMap");
        Objects.requireNonNull(mongoImageTag, "mongoImageTag");
        lastStartedAt = lastStartedAt == null ? Optional.empty() : lastStartedAt;
        lastStoppedAt = lastStoppedAt == null ? Optional.empty() : lastStoppedAt;
        destroyedAt = destroyedAt == null ? Optional.empty() : destroyedAt;
        connectionId = connectionId == null ? Optional.empty() : connectionId;
    }

    public Instant createdAtInstant() { return Instant.ofEpochMilli(createdAt); }
}
