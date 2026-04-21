package com.kubrik.mex.migration.profile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kubrik.mex.migration.spec.MigrationSpec;

import java.security.MessageDigest;
import java.util.HexFormat;

/** (De)serialises {@link MigrationSpec} to/from JSON and YAML.
 *  <p>
 *  Canonical mode (key-sorted, no flow aliases) drives the stable {@code specHash}
 *  used by the resume protocol (§4.3 of the tech spec). YAML is the preferred export
 *  format; both formats share the same schema. */
public final class ProfileCodec {

    private final ObjectMapper json;
    private final ObjectMapper yaml;
    private final ObjectMapper canonicalJson;

    public ProfileCodec() {
        this.json = baseMapper(new ObjectMapper());
        this.yaml = baseMapper(new ObjectMapper(new YAMLFactory()
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID)));
        this.canonicalJson = baseMapper(new ObjectMapper())
                .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    private static ObjectMapper baseMapper(ObjectMapper m) {
        return m.registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .enable(SerializationFeature.INDENT_OUTPUT);
    }

    public String toJson(MigrationSpec spec) throws Exception {
        return json.writeValueAsString(spec);
    }

    public String toYaml(MigrationSpec spec) throws Exception {
        return yaml.writeValueAsString(spec);
    }

    public MigrationSpec fromJson(String s) throws Exception {
        JsonNode tree = json.readTree(s);
        upgradeLegacyScope(tree);
        return json.treeToValue(tree, MigrationSpec.class);
    }

    public MigrationSpec fromYaml(String s) throws Exception {
        JsonNode tree = yaml.readTree(s);
        upgradeLegacyScope(tree);
        return yaml.treeToValue(tree, MigrationSpec.class);
    }

    /** v1.2.0 codec shim (spec §4.1). Rewrites legacy pre-v1.2.0 scope shapes in place so
     *  Jackson can deserialise them against the new {@link com.kubrik.mex.migration.spec.ScopeSpec}:
     *  <ul>
     *    <li>{@code mode: DATABASE, database: X} → {@code mode: DATABASES, databases: [X]}</li>
     *    <li>{@code mode: COLLECTIONS, database: X, namespaces: ["X.a"]} →
     *        {@code mode: COLLECTIONS, namespaces: [{db:X, coll:a}]}</li>
     *    <li>Flat {@code migrateIndexes: B} → {@code flags: {migrateIndexes: B, migrateUsers: false}}</li>
     *  </ul>
     *  Idempotent: already-v2 shapes round-trip unchanged. */
    static void upgradeLegacyScope(JsonNode root) {
        if (!(root instanceof ObjectNode specNode)) return;
        JsonNode scopeNode = specNode.get("scope");
        if (!(scopeNode instanceof ObjectNode scope)) return;

        String mode = scope.path("mode").asText(null);
        if ("DATABASE".equals(mode)) {
            scope.put("mode", "DATABASES");
            ArrayNode dbs = scope.arrayNode();
            JsonNode legacyDb = scope.remove("database");
            if (legacyDb != null && !legacyDb.isNull()) dbs.add(legacyDb.asText());
            scope.set("databases", dbs);
        } else if ("COLLECTIONS".equals(mode)) {
            String legacyDb = scope.path("database").asText(null);
            JsonNode namespaces = scope.get("namespaces");
            if (namespaces instanceof ArrayNode nsArr && nsArr.size() > 0 && nsArr.get(0).isTextual()) {
                // Rewrite ["db.coll", ...] → [{db, coll}, ...]
                ArrayNode upgraded = scope.arrayNode();
                for (JsonNode item : nsArr) {
                    String dotted = item.asText();
                    int dot = dotted.indexOf('.');
                    String db, coll;
                    if (dot > 0 && dot < dotted.length() - 1) {
                        db = dotted.substring(0, dot);
                        coll = dotted.substring(dot + 1);
                    } else if (legacyDb != null) {
                        db = legacyDb;
                        coll = dotted;
                    } else {
                        continue; // unresolvable — drop
                    }
                    ObjectNode ns = upgraded.objectNode();
                    ns.put("db", db);
                    ns.put("coll", coll);
                    upgraded.add(ns);
                }
                scope.set("namespaces", upgraded);
                scope.remove("database");
            }
        }

        // Flat migrateIndexes → flags object, for all three modes.
        if (scope.has("migrateIndexes") && !scope.has("flags")) {
            boolean migrateIndexes = scope.path("migrateIndexes").asBoolean(true);
            ObjectNode flags = scope.objectNode();
            flags.put("migrateIndexes", migrateIndexes);
            flags.put("migrateUsers", false);
            scope.set("flags", flags);
            scope.remove("migrateIndexes");
        }
    }

    /** SHA-256 over the canonical JSON serialisation of the identity subset of the spec.
     *  Used by the resume protocol to detect spec drift. */
    public String specHash(MigrationSpec spec) {
        try {
            // Only the identity-relevant fields go into the hash; tuning doesn't invalidate resume.
            IdentityView view = IdentityView.of(spec);
            byte[] canonical = canonicalJson.writeValueAsBytes(view);
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical);
            return "sha256:" + HexFormat.of().formatHex(digest);
        } catch (Exception e) {
            throw new RuntimeException("specHash failed: " + e.getMessage(), e);
        }
    }

    /** The subset of a spec that must remain stable for a resume to be valid. */
    private record IdentityView(
            int schema,
            String kind,
            String sourceConnectionId,
            String sourceReadPreference,
            String targetConnectionId,
            String targetDatabase,
            Object scope,
            String scriptsFolder,
            Object transform,
            Object conflict
    ) {
        static IdentityView of(MigrationSpec spec) {
            return new IdentityView(
                    spec.schema(),
                    spec.kind().name(),
                    spec.source().connectionId(),
                    spec.source().readPreference(),
                    spec.target().connectionId(),
                    spec.target().database(),
                    spec.scope(),
                    spec.scriptsFolder(),
                    spec.options().transform(),
                    spec.options().conflict());
        }
    }
}
