package com.kubrik.mex.maint.schema;

import com.kubrik.mex.maint.model.ValidatorSpec;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 SCHV-5 — Verifies the rollback command builder emits what
 * collMod expects; the live dispatch path is covered by the IT.
 */
class ValidatorRolloutRunnerTest {

    private final ValidatorRolloutRunner runner = new ValidatorRolloutRunner();

    @Test
    void rollback_command_round_trips_prior_state() {
        ValidatorSpec.Current prior = new ValidatorSpec.Current("app", "users",
                "{\"$jsonSchema\":{\"bsonType\":\"object\"}}",
                ValidatorSpec.Level.MODERATE, ValidatorSpec.Action.WARN);
        Document cmd = runner.buildRollbackCommand(prior);
        assertEquals("users", cmd.getString("collMod"));
        assertEquals("moderate", cmd.getString("validationLevel"));
        assertEquals("warn", cmd.getString("validationAction"));
        assertNotNull(cmd.get("validator", Document.class));
    }

    @Test
    void level_and_action_map_to_lowercase_wire_forms() {
        assertEquals("off", ValidatorRolloutRunner.levelToWire(ValidatorSpec.Level.OFF));
        assertEquals("strict", ValidatorRolloutRunner.levelToWire(ValidatorSpec.Level.STRICT));
        assertEquals("error", ValidatorRolloutRunner.actionToWire(ValidatorSpec.Action.ERROR));
        assertEquals("warn", ValidatorRolloutRunner.actionToWire(ValidatorSpec.Action.WARN));
    }
}
