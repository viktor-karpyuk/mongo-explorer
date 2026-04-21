package com.kubrik.mex.migration.versioned;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Filesystem loader for versioned-migration scripts.
 *  <p>
 *  Discovers {@code V{version}__{desc}.json} and the matching {@code U{version}__{desc}.json}
 *  rollback files in a folder. Rejects {@code .js} scripts with a pointed error (E-19) —
 *  JavaScript support is an MVP-Plus feature.
 *  <p>
 *  See docs/mvp-technical-spec.md §7.1. */
public final class ScriptRepo {

    private static final Pattern V_PATTERN = Pattern.compile(
            "^V(?<version>[0-9][0-9.]*)__(?<desc>.+)\\.json$");
    private static final Pattern U_PATTERN = Pattern.compile(
            "^U(?<version>[0-9][0-9.]*)__(?<desc>.+)\\.json$");
    private static final Pattern JS_PATTERN = Pattern.compile(
            "^[VU][0-9][0-9.]*__.+\\.js$");

    private final ObjectMapper mapper = new ObjectMapper();

    public record ScanResult(
            List<MigrationScript> scripts,        // V*, sorted by orderKey
            Map<String, MigrationScript> rollbacks, // keyed by version
            List<String> warnings,
            List<String> errors
    ) {
        public boolean hasErrors() { return !errors.isEmpty(); }
    }

    public ScanResult scan(Path folder) {
        List<MigrationScript> scripts = new ArrayList<>();
        Map<String, MigrationScript> rollbacks = new HashMap<>();
        List<String> warnings = new ArrayList<>();
        List<String> errors = new ArrayList<>();

        if (folder == null || !Files.isDirectory(folder)) {
            errors.add("Scripts folder does not exist: " + folder);
            return new ScanResult(scripts, rollbacks, warnings, errors);
        }

        Set<String> seenVersions = new HashSet<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(folder)) {
            for (Path p : stream) {
                String name = p.getFileName().toString();
                if (JS_PATTERN.matcher(name).matches()) {
                    warnings.add(".js scripts are not supported in this version (E-19): " + name);
                    continue;
                }
                Matcher vm = V_PATTERN.matcher(name);
                Matcher um = U_PATTERN.matcher(name);
                if (vm.matches()) {
                    parse(p, vm.group("version"), vm.group("desc"), errors)
                            .ifPresent(s -> {
                                if (!seenVersions.add(s.version())) {
                                    errors.add("Duplicate version `" + s.version() + "`: " + name);
                                } else {
                                    scripts.add(s);
                                }
                            });
                } else if (um.matches()) {
                    parse(p, um.group("version"), um.group("desc"), errors)
                            .ifPresent(s -> rollbacks.put(s.version(), s));
                }
                // Everything else is ignored silently (README files etc.)
            }
        } catch (IOException e) {
            errors.add("Cannot read folder: " + e.getMessage());
        }

        scripts.sort(Comparator.comparingLong(MigrationScript::orderKey));
        return new ScanResult(scripts, rollbacks, warnings, errors);
    }

    @SuppressWarnings("unchecked")
    private Optional<MigrationScript> parse(Path file, String version, String desc, List<String> errors) {
        try {
            byte[] raw = Files.readAllBytes(file);
            String checksum = "sha256:" + HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(raw));
            ObjectNode root = (ObjectNode) mapper.readTree(raw);
            ArrayNode opsNode = (ArrayNode) root.get("ops");
            if (opsNode == null) {
                errors.add("`ops` array missing in " + file.getFileName());
                return Optional.empty();
            }
            List<Op> ops = new ArrayList<>(opsNode.size());
            for (var opNode : opsNode) {
                ops.add(mapper.treeToValue(opNode, Op.class));
            }
            // VER-8 — optional `env` field gates the script to a named environment. Supports
            // positive (`prod`) and negated (`!prod`) forms; absent field means "everywhere".
            String envFilter = null;
            if (root.has("env") && !root.get("env").isNull()) {
                envFilter = root.get("env").asText();
            }
            return Optional.of(new MigrationScript(
                    version,
                    MigrationScript.computeOrderKey(version),
                    desc,
                    checksum,
                    List.copyOf(ops),
                    file,
                    envFilter));
        } catch (Exception e) {
            errors.add(file.getFileName() + ": " + e.getMessage());
            return Optional.empty();
        }
    }
}
