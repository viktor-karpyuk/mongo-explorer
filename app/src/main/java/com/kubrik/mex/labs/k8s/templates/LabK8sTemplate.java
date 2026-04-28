package com.kubrik.mex.labs.k8s.templates;

import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.labs.k8s.model.LabK8sDistro;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * v2.8.1 Q2.8-N3 — Blessed K8s Labs template.
 *
 * <p>A template is a display-level record + a factory that produces
 * a {@link ProvisionModel} when the user picks it. All rendering /
 * applying / rolling out happens through the v2.8.1 production
 * pipeline — templates are strictly curated inputs, not a parallel
 * renderer (Decision 11).</p>
 *
 * <p>The five day-one templates span both operators + the two
 * headline extras (sharding, backup, TLS+keyfile) so the curriculum
 * touches every CR shape the adapters know.</p>
 */
public record LabK8sTemplate(
        String id,
        String displayName,
        String description,
        Set<LabK8sDistro> supportedDistros,
        List<String> tags,
        TemplateFactory factory) {

    public LabK8sTemplate {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(displayName, "displayName");
        Objects.requireNonNull(description, "description");
        Objects.requireNonNull(supportedDistros, "supportedDistros");
        Objects.requireNonNull(tags, "tags");
        Objects.requireNonNull(factory, "factory");
        if (id.isBlank()) throw new IllegalArgumentException("id blank");
        supportedDistros = Set.copyOf(supportedDistros);
        tags = List.copyOf(tags);
    }

    /**
     * Factory seam — the UI calls this with the cluster ref id +
     * target namespace to materialise a ready-to-apply model. The
     * factory is pure: no I/O, no randomness, no clock reads.
     */
    @FunctionalInterface
    public interface TemplateFactory {
        ProvisionModel build(long clusterId, String namespace, String deploymentName);
    }
}
