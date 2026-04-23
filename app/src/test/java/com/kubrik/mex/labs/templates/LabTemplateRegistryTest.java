package com.kubrik.mex.labs.templates;

import com.kubrik.mex.labs.model.LabTemplate;
import com.kubrik.mex.labs.model.SeedSpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LabTemplateRegistryTest {

    @Test
    void loads_six_builtins_in_declared_order() {
        LabTemplateRegistry r = new LabTemplateRegistry();
        r.loadBuiltins();
        var names = r.all().stream().map(LabTemplate::id).toList();
        assertEquals(LabTemplateRegistry.BUILTIN_IDS, names);
    }

    @Test
    void each_template_declares_at_least_one_container() {
        LabTemplateRegistry r = new LabTemplateRegistry();
        r.loadBuiltins();
        for (LabTemplate t : r.all()) {
            assertFalse(t.containerNames().isEmpty(),
                    "template " + t.id() + " must declare containers");
        }
    }

    @Test
    void sample_mflix_has_seed_spec_with_target_db() {
        LabTemplateRegistry r = new LabTemplateRegistry();
        r.loadBuiltins();
        LabTemplate t = r.byId("sample-mflix").orElseThrow();
        assertTrue(t.seedSpec().isPresent());
        SeedSpec s = t.seedSpec().get();
        assertEquals(SeedSpec.Kind.FETCH_ON_DEMAND, s.kind());
        assertEquals("sample_mflix", s.targetDb());
    }

    @Test
    void sharded_template_has_mongos_container() {
        LabTemplateRegistry r = new LabTemplateRegistry();
        r.loadBuiltins();
        LabTemplate t = r.byId("sharded-1").orElseThrow();
        assertTrue(t.containerNames().contains("mongos"));
        assertTrue(t.containerNames().contains("cfg1"));
        assertTrue(t.composeTemplate().contains("{{ports.mongos}}"));
    }

    @Test
    void triple_rs_declares_three_independent_seeds() {
        LabTemplateRegistry r = new LabTemplateRegistry();
        r.loadBuiltins();
        LabTemplate t = r.byId("triple-rs").orElseThrow();
        // Nine mongod containers + one init container.
        assertTrue(t.containerNames().contains("rs1a"));
        assertTrue(t.containerNames().contains("rs2a"));
        assertTrue(t.containerNames().contains("rs3a"));
    }

    @Test
    void unknown_schema_version_is_rejected() {
        String badYaml = """
                schema_version: 99
                id: bogus
                containers:
                  - x
                compose_template: |
                  name: {{projectName}}
                  services:
                    x:
                      image: mongo
                """;
        assertThrows(IllegalStateException.class,
                () -> LabTemplateRegistry.parse("bogus", badYaml));
    }
}
