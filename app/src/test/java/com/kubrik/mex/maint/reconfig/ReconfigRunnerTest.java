package com.kubrik.mex.maint.reconfig;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 Q2.7-D — covers the non-dispatch surface on
 * {@link ReconfigRunner} (the rollback plan builder). Live dispatch
 * paths are covered by {@code ReconfigPreflightIT} + the upcoming
 * 3-node IT rig.
 */
class ReconfigRunnerTest {

    private final ReconfigRunner runner = new ReconfigRunner();

    @Test
    void null_reply_produces_empty_plan() {
        assertEquals("{}", runner.buildRollbackPlanJson(null));
    }

    @Test
    void reply_without_config_yields_command_only_plan() {
        String json = runner.buildRollbackPlanJson(new Document());
        // Shape: { "command": "replSetReconfig" } — no priorConfig
        // field at all. Previously the code path round-tripped
        // through a dummy WholeConfig which is a foot-gun; this
        // form is a clean passthrough.
        assertTrue(json.contains("replSetReconfig"));
        assertFalse(json.contains("priorConfig"));
    }

    @Test
    void reply_with_config_round_trips_priorConfig() {
        Document cfg = new Document("_id", "rs0")
                .append("version", 9)
                .append("members", List.of(
                        new Document("_id", 0).append("host", "h1:27017")));
        String json = runner.buildRollbackPlanJson(
                new Document("config", cfg));
        assertTrue(json.contains("priorConfig"));
        assertTrue(json.contains("rs0"));
        assertTrue(json.contains("h1:27017"));
    }
}
