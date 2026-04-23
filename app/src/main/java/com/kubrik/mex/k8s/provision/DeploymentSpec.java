package com.kubrik.mex.k8s.provision;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-J — Portable deployment descriptor for clone +
 * export.
 *
 * <p>Written by {@link DeploymentSpecExporter}; consumed by
 * {@link DeploymentSpecImporter} to pre-fill the wizard. Wraps a
 * {@link ProvisionModel} (minus sensitive fields) + metadata the
 * audit trail wants.</p>
 *
 * <p>Deliberately JSON-serialisable: Jackson's record support
 * round-trips every field. Passwords are always omitted — the
 * exporter strips them; the importer re-prompts the user.</p>
 */
public record DeploymentSpec(
        String schemaVersion,
        String exportedAt,
        Optional<String> sourceDeployment,
        Optional<String> evidenceSig,
        ProvisionModel model) {

    public static final String CURRENT_SCHEMA = "mex.k8s/v2.8.1";

    public DeploymentSpec {
        Objects.requireNonNull(schemaVersion, "schemaVersion");
        Objects.requireNonNull(exportedAt, "exportedAt");
        Objects.requireNonNull(sourceDeployment, "sourceDeployment");
        Objects.requireNonNull(evidenceSig, "evidenceSig");
        Objects.requireNonNull(model, "model");
    }
}
