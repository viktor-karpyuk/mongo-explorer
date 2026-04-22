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
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 Q2.7-A — Round-trips a rollback plan against a seeded
 * {@code ops_audit} row, and asserts the missing-audit-row guard.
 */
class RollbackPlanWriterTest {

    @TempDir Path dataDir;

    private Database db;
    private RollbackPlanWriter writer;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        writer = new RollbackPlanWriter(db);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void round_trips_plan_for_existing_audit_row() {
        long auditId = seedAudit("rs.reconfig");
        long planId = writer.write(new RollbackPlan.Input(auditId,
                RollbackPlan.Kind.RS_CONFIG,
                "{\"command\":\"rs.reconfig\",\"priorConfig\":{...}}",
                null));
        assertTrue(planId > 0);

        Optional<RollbackPlan.Row> row = writer.byId(planId);
        assertTrue(row.isPresent());
        assertEquals(RollbackPlan.Kind.RS_CONFIG, row.get().kind());
        assertEquals(auditId, row.get().auditId());
        assertNull(row.get().appliedAt());
    }

    @Test
    void refuses_plan_for_missing_audit_row() {
        // A rollback plan without an audit row is an inconsistent
        // state — caller bug, not a DB miss.
        assertThrows(IllegalStateException.class,
                () -> writer.write(new RollbackPlan.Input(9_999L,
                        RollbackPlan.Kind.PARAM,
                        "{\"param\":\"wiredTigerConcurrentReadTransactions\"}",
                        null)));
    }

    @Test
    void markApplied_records_outcome() {
        long auditId = seedAudit("param.set");
        long planId = writer.write(new RollbackPlan.Input(auditId,
                RollbackPlan.Kind.PARAM,
                "{\"param\":\"foo\",\"priorValue\":42}", null));
        assertTrue(writer.markApplied(planId, RollbackPlan.Outcome.OK,
                1_700_000_000_000L, "applied on 2026-04-22"));

        RollbackPlan.Row row = writer.byId(planId).orElseThrow();
        assertEquals(Optional.of(RollbackPlan.Outcome.OK), row.outcome());
        assertEquals(1_700_000_000_000L, row.appliedAt());
        assertTrue(row.notes().contains("applied on 2026-04-22"));
    }

    @Test
    void markApplied_is_idempotent_but_appends_notes() {
        long auditId = seedAudit("param.set");
        long planId = writer.write(new RollbackPlan.Input(auditId,
                RollbackPlan.Kind.PARAM, "{\"k\":\"v\"}", "initial"));
        writer.markApplied(planId, RollbackPlan.Outcome.FAIL,
                1_700_000_000_000L, "try 1 failed");
        writer.markApplied(planId, RollbackPlan.Outcome.OK,
                1_700_000_100_000L, "try 2 ok");
        RollbackPlan.Row row = writer.byId(planId).orElseThrow();
        // Latest call wins for outcome + appliedAt; notes accumulate
        // so the replay history stays inspectable.
        assertEquals(Optional.of(RollbackPlan.Outcome.OK), row.outcome());
        assertEquals(1_700_000_100_000L, row.appliedAt());
        assertTrue(row.notes().contains("initial"));
        assertTrue(row.notes().contains("try 1 failed"));
        assertTrue(row.notes().contains("try 2 ok"));
    }

    @Test
    void byAuditId_returns_multiple_plans_in_order() {
        long auditId = seedAudit("rs.reconfig");
        long a = writer.write(new RollbackPlan.Input(auditId,
                RollbackPlan.Kind.RS_CONFIG, "{\"step\":1}", null));
        long b = writer.write(new RollbackPlan.Input(auditId,
                RollbackPlan.Kind.RS_CONFIG, "{\"step\":2}", null));
        List<RollbackPlan.Row> rows = writer.byAuditId(auditId);
        assertEquals(2, rows.size());
        assertEquals(a, rows.get(0).id());
        assertEquals(b, rows.get(1).id());
    }

    /* ============================ fixtures ============================ */

    private long seedAudit(String commandName) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "INSERT INTO ops_audit(connection_id, command_name, " +
                "command_json_redacted, preview_hash, outcome, started_at, " +
                "ui_source) VALUES (?, ?, ?, ?, ?, ?, ?)",
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, "cx-1");
            ps.setString(2, commandName);
            ps.setString(3, "{\"redacted\":true}");
            ps.setString(4, "SHA256-preview");
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
