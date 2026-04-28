package com.kubrik.mex.migration.profile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kubrik.mex.migration.spec.ConflictMode;
import com.kubrik.mex.migration.spec.ErrorPolicy;
import com.kubrik.mex.migration.spec.ExecutionMode;
import com.kubrik.mex.migration.spec.MigrationKind;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.migration.spec.Namespace;
import com.kubrik.mex.migration.spec.PerfSpec;
import com.kubrik.mex.migration.spec.ScopeFlags;
import com.kubrik.mex.migration.spec.ScopeSpec;
import com.kubrik.mex.migration.spec.SourceSpec;
import com.kubrik.mex.migration.spec.TargetSpec;
import com.kubrik.mex.migration.spec.VerifySpec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** P3 ProfileCodec v1 → v2 shim — see spec §4.1. Ensures pre-v1.2.0 profiles still load and
 *  round-trip into the new {@link ScopeSpec} shape.
 *
 *  <p>Rather than hand-craft JSON (brittle against incidental spec tweaks), we serialize a
 *  known-good v2 spec, mutate the scope node into legacy shape, then re-deserialize and
 *  assert the shim rewrote it. */
class ProfileCodecShimTest {

    private final ProfileCodec codec = new ProfileCodec();
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void upgrades_legacy_database_scope() throws Exception {
        MigrationSpec v2 = specWith(new ScopeSpec.Databases(List.of("acme_crm"),
                new ScopeFlags(true, false), List.of("**"), List.of(), List.of()));
        String json = codec.toJson(v2);
        JsonNode tree = mapper.readTree(json);
        ObjectNode scope = (ObjectNode) tree.get("scope");
        scope.put("mode", "DATABASE");
        scope.put("database", "acme_crm");
        scope.remove("databases");
        scope.remove("flags");
        scope.put("migrateIndexes", true);

        var upgraded = codec.fromJson(mapper.writeValueAsString(tree));
        assertInstanceOf(ScopeSpec.Databases.class, upgraded.scope());
        ScopeSpec.Databases d = (ScopeSpec.Databases) upgraded.scope();
        assertEquals(List.of("acme_crm"), d.databases());
        assertTrue(d.flags().migrateIndexes());
        assertFalse(d.flags().migrateUsers());
    }

    @Test
    void upgrades_legacy_collections_scope_with_dotted_namespace_strings() throws Exception {
        MigrationSpec v2 = specWith(new ScopeSpec.Collections(
                List.of(new Namespace("acme_crm", "users"), new Namespace("acme_crm", "orders")),
                new ScopeFlags(false, false), List.of("**"), List.of(), List.of()));
        String json = codec.toJson(v2);
        JsonNode tree = mapper.readTree(json);
        ObjectNode scope = (ObjectNode) tree.get("scope");
        scope.put("database", "acme_crm");
        ArrayNode nsArr = scope.arrayNode();
        nsArr.add("acme_crm.users"); nsArr.add("acme_crm.orders");
        scope.set("namespaces", nsArr);
        scope.remove("flags");
        scope.put("migrateIndexes", false);

        var upgraded = codec.fromJson(mapper.writeValueAsString(tree));
        assertInstanceOf(ScopeSpec.Collections.class, upgraded.scope());
        ScopeSpec.Collections c = (ScopeSpec.Collections) upgraded.scope();
        assertEquals(2, c.namespaces().size());
        assertEquals(new Namespace("acme_crm", "users"), c.namespaces().get(0));
        assertEquals(new Namespace("acme_crm", "orders"), c.namespaces().get(1));
        assertFalse(c.flags().migrateIndexes());
        assertFalse(c.flags().migrateUsers(),
                "legacy profiles default migrateUsers to false per §4.1");
    }

    @Test
    void upgrades_legacy_server_scope_flags() throws Exception {
        MigrationSpec v2 = specWith(new ScopeSpec.Server(
                new ScopeFlags(true, false), List.of("**"), List.of(), List.of()));
        String json = codec.toJson(v2);
        JsonNode tree = mapper.readTree(json);
        ObjectNode scope = (ObjectNode) tree.get("scope");
        scope.remove("flags");
        scope.put("migrateIndexes", true);

        var upgraded = codec.fromJson(mapper.writeValueAsString(tree));
        assertInstanceOf(ScopeSpec.Server.class, upgraded.scope());
        ScopeSpec.Server s = (ScopeSpec.Server) upgraded.scope();
        assertTrue(s.flags().migrateIndexes());
        assertFalse(s.flags().migrateUsers());
    }

    @Test
    void round_trips_v2_shape_unchanged() throws Exception {
        MigrationSpec v2 = specWith(new ScopeSpec.Databases(List.of("a", "b"),
                new ScopeFlags(true, true), List.of("**"), List.of(), List.of()));
        String reserialised = codec.toJson(v2);
        var roundTripped = codec.fromJson(reserialised);
        assertEquals(v2.scope(), roundTripped.scope());
    }

    private MigrationSpec specWith(ScopeSpec scope) {
        return new MigrationSpec(
                1, MigrationKind.DATA_TRANSFER, "legacy-probe",
                new SourceSpec("src-id", "primary"),
                new TargetSpec("tgt-id", null),
                scope,
                null,
                new MigrationSpec.Options(
                        ExecutionMode.RUN,
                        new MigrationSpec.Conflict(ConflictMode.ABORT, Map.of()),
                        Map.of(),
                        PerfSpec.defaults(),
                        VerifySpec.defaults(),
                        ErrorPolicy.defaults(), false, null, List.of()));
    }
}
