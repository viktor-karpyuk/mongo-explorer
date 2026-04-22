package com.kubrik.mex.maint.rollback;

import com.kubrik.mex.maint.model.RollbackPlan;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class RollbackReplayServiceTest {

    @TempDir Path dataDir;

    private Database db;
    private RollbackPlanWriter writer;
    private RollbackReplayService svc;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        writer = new RollbackPlanWriter(db);
        svc = new RollbackReplayService(writer);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void lookup_unpacks_plan_row_into_replay_request() {
        long auditId = seedAudit();
        long planId = writer.write(new RollbackPlan.Input(auditId,
                RollbackPlan.Kind.PARAM,
                "{\"param\":\"notablescan\",\"priorValue\":false}",
                null));

        Optional<RollbackReplayService.ReplayRequest> req = svc.lookup(planId);
        assertTrue(req.isPresent());
        assertEquals(RollbackPlan.Kind.PARAM, req.get().kind());
        assertFalse(req.get().alreadyApplied());
        assertTrue(req.get().planJson().contains("notablescan"));
    }

    @Test
    void lookup_flags_alreadyApplied_after_markApplied() {
        long auditId = seedAudit();
        long planId = writer.write(new RollbackPlan.Input(auditId,
                RollbackPlan.Kind.VALIDATOR, "{\"collMod\":\"users\"}", null));
        svc.recordOutcome(planId, RollbackPlan.Outcome.OK, "replayed");

        RollbackReplayService.ReplayRequest req = svc.lookup(planId).orElseThrow();
        assertTrue(req.alreadyApplied(),
                "UI should warn before replaying an already-applied plan");
    }

    @Test
    void lookup_of_missing_plan_is_empty() {
        assertTrue(svc.lookup(9_999L).isEmpty());
    }

    @Test
    void lookupByAuditId_returns_first_plan_for_multi_plan_actions() {
        long auditId = seedAudit();
        long first = writer.write(new RollbackPlan.Input(auditId,
                RollbackPlan.Kind.RS_CONFIG, "{\"step\":1}", null));
        writer.write(new RollbackPlan.Input(auditId,
                RollbackPlan.Kind.RS_CONFIG, "{\"step\":2}", null));

        RollbackReplayService.ReplayRequest req = svc.lookupByAuditId(auditId)
                .orElseThrow();
        assertEquals(first, req.planId());
    }

    private long seedAudit() {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "INSERT INTO ops_audit(connection_id, command_name, " +
                "command_json_redacted, preview_hash, outcome, started_at, " +
                "ui_source) VALUES (?, ?, ?, ?, ?, ?, ?)",
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, "cx-1");
            ps.setString(2, "rs.reconfig");
            ps.setString(3, "{\"redacted\":true}");
            ps.setString(4, "hash");
            ps.setString(5, "OK");
            ps.setLong(6, System.currentTimeMillis());
            ps.setString(7, "test");
            ps.executeUpdate();
            try (var keys = ps.getGeneratedKeys()) {
                return keys.next() ? keys.getLong(1) : -1L;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
