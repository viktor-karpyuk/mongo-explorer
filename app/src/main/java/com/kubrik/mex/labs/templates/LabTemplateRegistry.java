package com.kubrik.mex.labs.templates;

import com.kubrik.mex.labs.model.LabTemplate;
import com.kubrik.mex.labs.model.SeedSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * v2.8.4 Q2.8.4-B — Loads template manifests from
 * {@code labs/templates/*.yaml} on the classpath at startup.
 *
 * <p>Adding a 7th template is a YAML file + a golden test fixture —
 * zero Java code change per milestone decision 4.</p>
 *
 * <p>The YAML shape is pinned at schema_version: 1. A template with
 * an unknown schema_version is skipped with a log warning (forward
 * compat: a newer Mongo Explorer may emit templates this reader
 * doesn't understand).</p>
 */
public final class LabTemplateRegistry {

    private static final Logger log = LoggerFactory.getLogger(LabTemplateRegistry.class);

    /** Which YAML shape this reader understands. */
    public static final int READER_SCHEMA = 1;

    /** Catalogue ids shipped in v2.8.4.0 (milestone §1.7). Load
     *  order is the UI display order in the Labs tab catalogue. */
    public static final List<String> BUILTIN_IDS = List.of(
            "standalone", "rs-3", "rs-5", "sharded-1",
            "triple-rs", "sample-mflix");

    private final Map<String, LabTemplate> byId = new LinkedHashMap<>();

    /** Load every {@link #BUILTIN_IDS} entry from
     *  {@code /labs/templates/<id>.yaml}. Missing files are a
     *  packaging bug — throw so a broken jar doesn't silently ship
     *  with an empty catalogue. */
    public void loadBuiltins() {
        for (String id : BUILTIN_IDS) {
            String path = "/labs/templates/" + id + ".yaml";
            try (InputStream in = LabTemplateRegistry.class.getResourceAsStream(path)) {
                if (in == null) {
                    throw new IllegalStateException(
                            "template resource missing: " + path);
                }
                String yaml = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                LabTemplate t = parse(id, yaml);
                byId.put(id, t);
                log.info("loaded lab template {}", id);
            } catch (IOException ioe) {
                throw new IllegalStateException(
                        "failed to load template " + id, ioe);
            }
        }
    }

    public Optional<LabTemplate> byId(String id) {
        return Optional.ofNullable(byId.get(id));
    }

    public List<LabTemplate> all() { return new ArrayList<>(byId.values()); }

    /* ============================ parser ============================ */

    /**
     * Minimal YAML reader — we only need to extract the fixed set of
     * template fields, and we write the manifests ourselves, so a
     * full YAML library is overkill. Handles: scalar fields,
     * list-of-strings under {@code containers:}, and a
     * {@code compose_template: |} block scalar with 2-space dedent.
     */
    static LabTemplate parse(String id, String yaml) {
        Map<String, String> scalars = new LinkedHashMap<>();
        List<String> containers = new ArrayList<>();
        String composeBlock = null;
        Optional<SeedSpec> seed = Optional.empty();

        String[] lines = yaml.split("\\r?\\n", -1);
        int i = 0;
        while (i < lines.length) {
            String line = lines[i];
            if (line.isBlank() || line.startsWith("#")) { i++; continue; }
            int colon = line.indexOf(':');
            if (colon < 0) { i++; continue; }
            String key = line.substring(0, colon).trim();
            String rest = line.substring(colon + 1).trim();

            if (key.equals("compose_template") && rest.equals("|")) {
                // Block scalar — every subsequent indented line is
                // part of the value. Indent = first non-blank line's
                // leading-space count.
                StringBuilder sb = new StringBuilder();
                int indent = -1;
                i++;
                while (i < lines.length) {
                    String bl = lines[i];
                    int leading = 0;
                    while (leading < bl.length() && bl.charAt(leading) == ' ') leading++;
                    if (bl.isBlank()) {
                        // blank lines inside the block are kept as-is
                        sb.append('\n');
                        i++;
                        continue;
                    }
                    if (indent < 0) indent = leading;
                    if (leading < indent) break;  // end of block
                    sb.append(bl, indent, bl.length()).append('\n');
                    i++;
                }
                composeBlock = sb.toString();
                continue;
            }
            if (key.equals("containers")) {
                i++;
                while (i < lines.length && lines[i].startsWith("  - ")) {
                    containers.add(lines[i].substring(4).trim());
                    i++;
                }
                continue;
            }
            if (key.equals("seed_spec")) {
                Map<String, String> fields = new LinkedHashMap<>();
                i++;
                while (i < lines.length && lines[i].startsWith("  ")) {
                    String sl = lines[i].substring(2);
                    int sc = sl.indexOf(':');
                    if (sc > 0) fields.put(sl.substring(0, sc).trim(),
                            sl.substring(sc + 1).trim().replaceAll("^[\"']|[\"']$", ""));
                    i++;
                }
                seed = Optional.of(new SeedSpec(
                        SeedSpec.Kind.valueOf(
                                fields.getOrDefault("kind", "BUNDLED")),
                        fields.getOrDefault("locator", ""),
                        fields.getOrDefault("target_db", "test"),
                        Optional.ofNullable(fields.get("sha256"))
                                .filter(s -> !s.isBlank())));
                continue;
            }

            // Plain scalar line — strip matching quotes.
            scalars.put(key, rest.replaceAll("^[\"']|[\"']$", ""));
            i++;
        }

        int schema = parseInt(scalars.get("schema_version"), 1);
        if (schema != READER_SCHEMA) {
            throw new IllegalStateException("template " + id
                    + " schema_version=" + schema
                    + " — reader supports " + READER_SCHEMA);
        }
        if (composeBlock == null) {
            throw new IllegalStateException("template " + id
                    + " missing compose_template block");
        }
        if (containers.isEmpty()) {
            throw new IllegalStateException("template " + id
                    + " must declare at least one container in `containers:`");
        }

        return new LabTemplate(
                id,
                scalars.getOrDefault("display_name", id),
                scalars.getOrDefault("description", ""),
                parseInt(scalars.get("est_memory_mib"), 0),
                parseInt(scalars.get("est_startup_seconds"), 30),
                scalars.getOrDefault("default_mongo_tag", "mongo:latest"),
                containers,
                composeBlock,
                seed,
                schema);
    }

    private static int parseInt(String s, int fallback) {
        if (s == null) return fallback;
        try { return Integer.parseInt(s.trim()); }
        catch (NumberFormatException e) { return fallback; }
    }
}
