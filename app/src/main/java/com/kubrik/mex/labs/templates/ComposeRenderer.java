package com.kubrik.mex.labs.templates;

import com.kubrik.mex.labs.model.LabTemplate;
import com.kubrik.mex.labs.model.PortMap;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * v2.8.4 Q2.8.4-B — Renders a {@link LabTemplate}'s compose-template
 * string into a concrete {@code docker-compose.yml} on disk.
 *
 * <p>Deterministic: same inputs → same bytes, so the golden tests
 * byte-compare. A SHA-256 of the rendered file is stored in the
 * {@code lab_deployments} row for integrity (analogous to v2.8.0's
 * {@code PreviewHashChecker}).</p>
 *
 * <p>Placeholder grammar (Mustache-ish, minimal, no loops / sections):
 * <ul>
 *   <li>{@code {{projectName}}} — the compose project name</li>
 *   <li>{@code {{mongoTag}}} — the mongod image tag</li>
 *   <li>{@code {{ports.<containerName>}}} — host port for a container</li>
 *   <li>{@code {{authEnabled}}} — boolean, rendered as {@code true} / {@code false}</li>
 * </ul>
 *
 * <p>We don't pull in full Mustache-java — a tight tag grammar is
 * enough and keeps the jpackage image lean. Unknown placeholders
 * left unrendered trip the post-render YAML parse guard so a
 * malformed template fails fast at render time, not compose-up time.</p>
 */
public final class ComposeRenderer {

    /** Render the template into {@code dataDir/<projectName>/docker-compose.yml}
     *  and return the file path. */
    public Path render(LabTemplate template, PortMap ports, String mongoTag,
                        boolean authEnabled, String projectName,
                        Path dataDir) throws IOException {
        String rendered = renderString(template, ports, mongoTag,
                authEnabled, projectName);
        assertParsesAsYaml(rendered, template.id());

        Path outDir = dataDir.resolve(projectName);
        Files.createDirectories(outDir);
        Path out = outDir.resolve("docker-compose.yml");
        Files.writeString(out, rendered, StandardCharsets.UTF_8);
        return out;
    }

    /** Pure string form — exposed for the golden tests. Validates
     *  that no {@code {{…}}} placeholders remain unreplaced. */
    public String renderString(LabTemplate template, PortMap ports,
                                String mongoTag, boolean authEnabled,
                                String projectName) {
        String body = template.composeTemplate();
        body = body.replace("{{projectName}}", projectName);
        body = body.replace("{{mongoTag}}", mongoTag);
        body = body.replace("{{authEnabled}}", Boolean.toString(authEnabled));
        for (Map.Entry<String, Integer> e : ports.ports().entrySet()) {
            body = body.replace("{{ports." + e.getKey() + "}}",
                    Integer.toString(e.getValue()));
        }
        assertNoUnrendered(body, template.id());
        return body;
    }

    private static void assertNoUnrendered(String body, String templateId) {
        int idx = body.indexOf("{{");
        if (idx < 0) return;
        int end = body.indexOf("}}", idx);
        String tag = end < 0 ? body.substring(idx) : body.substring(idx, end + 2);
        throw new IllegalStateException("template " + templateId
                + " left an unrendered placeholder: " + tag);
    }

    /** Cheap YAML well-formedness check — we don't need a full
     *  parser, just the compose-up-would-accept-this invariant.
     *  Look for the top-level {@code services:} key and reject
     *  obvious tab indents that would break YAML parsers. */
    private static void assertParsesAsYaml(String body, String templateId) {
        if (!body.contains("services:")) {
            throw new IllegalStateException("template " + templateId
                    + " missing required top-level `services:`");
        }
        // YAML forbids tab indentation; a stray tab somewhere in the
        // template body breaks the compose parser with an obscure
        // error. Catch it here with a clear message.
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '\t') {
                int line = 1;
                for (int j = 0; j < i; j++) if (body.charAt(j) == '\n') line++;
                throw new IllegalStateException("template " + templateId
                        + " contains a tab at line " + line
                        + " — YAML requires spaces for indentation");
            }
        }
    }
}
