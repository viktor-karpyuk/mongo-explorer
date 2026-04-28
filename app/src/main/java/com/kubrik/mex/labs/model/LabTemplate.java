package com.kubrik.mex.labs.model;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.4 Q2.8.4-B — One catalogue entry loaded from
 * {@code src/main/resources/labs/templates/<id>.yaml}. Pure data:
 * the compose-template string is Mustache-over-YAML and gets
 * rendered by {@code ComposeRenderer} — this record just carries it.
 */
public record LabTemplate(
        String id,
        String displayName,
        String description,
        int estMemoryMib,
        int estStartupSeconds,
        String defaultMongoTag,
        /** Ordered list of container names — drives port allocation
         *  order so the generated port_map_json is stable. */
        java.util.List<String> containerNames,
        /** Mustache-over-YAML compose template. Placeholders:
         *  {@code {{projectName}}}, {@code {{mongoTag}}},
         *  {@code {{ports.<containerName>}}}. */
        String composeTemplate,
        Optional<SeedSpec> seedSpec,
        int schemaVersion
) {
    public LabTemplate {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(composeTemplate, "composeTemplate");
        containerNames = java.util.List.copyOf(containerNames);
        seedSpec = seedSpec == null ? Optional.empty() : seedSpec;
    }
}
