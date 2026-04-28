package com.kubrik.mex.backup.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kubrik.mex.backup.spec.ArchiveSpec;
import com.kubrik.mex.backup.spec.RetentionSpec;
import com.kubrik.mex.backup.spec.Scope;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * v2.5 Q2.5-A — JSON codecs for the three sub-records stored inside
 * {@code backup_policies} as TEXT blobs (scope_json, archive_json,
 * retention_json). Kept small + dependency-free (only Jackson, which the
 * project already uses for migration configs) so the DAO stays tidy.
 */
final class BackupPolicyCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private BackupPolicyCodec() {}

    /* =============================== scope =============================== */

    static String encodeScope(Scope scope) {
        ObjectNode node = MAPPER.createObjectNode();
        switch (scope) {
            case Scope.WholeCluster w -> node.put("kind", "whole_cluster");
            case Scope.Databases d -> {
                node.put("kind", "databases");
                var arr = node.putArray("names");
                for (String n : d.names()) arr.add(n);
            }
            case Scope.Namespaces ns -> {
                node.put("kind", "namespaces");
                var arr = node.putArray("namespaces");
                for (String n : ns.namespaces()) arr.add(n);
            }
        }
        return node.toString();
    }

    static Scope decodeScope(String json) {
        try {
            JsonNode node = MAPPER.readTree(json);
            String kind = node.path("kind").asText();
            return switch (kind) {
                case "whole_cluster" -> new Scope.WholeCluster();
                case "databases" -> new Scope.Databases(readStrings(node.path("names")));
                case "namespaces" -> new Scope.Namespaces(readStrings(node.path("namespaces")));
                default -> throw new IllegalArgumentException("unknown scope kind: " + kind);
            };
        } catch (IOException e) {
            throw new IllegalArgumentException("scope json: " + json, e);
        }
    }

    /* ============================== archive ============================== */

    static String encodeArchive(ArchiveSpec spec) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("gzip", spec.gzip());
        n.put("level", spec.level());
        n.put("outputDirTemplate", spec.outputDirTemplate());
        return n.toString();
    }

    static ArchiveSpec decodeArchive(String json) {
        try {
            JsonNode n = MAPPER.readTree(json);
            boolean gzip = n.path("gzip").asBoolean(true);
            // The record rejects level < 1 when gzip is off, so coerce to 6 when absent.
            int level = n.path("level").asInt(gzip ? 6 : 0);
            String tmpl = n.path("outputDirTemplate").asText(ArchiveSpec.DEFAULT_TEMPLATE);
            return new ArchiveSpec(gzip, level, tmpl);
        } catch (IOException e) {
            throw new IllegalArgumentException("archive json: " + json, e);
        }
    }

    /* ============================= retention ============================= */

    static String encodeRetention(RetentionSpec spec) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("maxCount", spec.maxCount());
        n.put("maxAgeDays", spec.maxAgeDays());
        return n.toString();
    }

    static RetentionSpec decodeRetention(String json) {
        try {
            JsonNode n = MAPPER.readTree(json);
            return new RetentionSpec(
                    n.path("maxCount").asInt(30),
                    n.path("maxAgeDays").asInt(30));
        } catch (IOException e) {
            throw new IllegalArgumentException("retention json: " + json, e);
        }
    }

    private static List<String> readStrings(JsonNode arr) {
        List<String> out = new ArrayList<>();
        if (arr.isArray()) for (JsonNode item : arr) out.add(item.asText());
        return out;
    }
}
