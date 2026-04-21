package com.kubrik.mex.backup;

import com.kubrik.mex.backup.rehearse.DrRehearsalReport;
import com.kubrik.mex.cluster.audit.OpsAuditRecord;
import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.store.OpsAuditDao;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.5 Q2.5-G — covers the report builder: counts by outcome, JSON +
 * HTML output shape, and filtering (non-rehearse rows are skipped even
 * when returned by listSince).
 */
class DrRehearsalReportTest {

    @TempDir Path dataDir;
    @TempDir Path out;

    private Database db;
    private OpsAuditDao audit;
    private DrRehearsalReport report;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        audit = new OpsAuditDao(db);
        report = new DrRehearsalReport(audit);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void bundle_counts_outcomes_and_skips_non_rehearse_rows() {
        seedRehearse(1_000L, Outcome.OK);
        seedRehearse(2_000L, Outcome.FAIL);
        seedRehearse(3_000L, Outcome.CANCELLED);
        seedOther(4_000L, "killOp", Outcome.OK);

        DrRehearsalReport.Bundle b = report.build(0L, 100);
        assertEquals(3, b.rowCount(), "other command_names filtered out");
        assertEquals(1, b.ok());
        assertEquals(1, b.fail());
        assertEquals(1, b.cancelled());
    }

    @Test
    void writeJson_emits_self_describing_bundle() throws Exception {
        seedRehearse(1_000L, Outcome.OK);
        DrRehearsalReport.Bundle b = report.build(0L, 100);
        Path f = out.resolve("report.json");
        report.writeJson(b, f);
        String body = Files.readString(f, StandardCharsets.UTF_8);
        assertTrue(body.contains("\"generator\""));
        assertTrue(body.contains("\"ok\":1"));
        assertTrue(body.contains("\"rows\":["));
    }

    @Test
    void writeHtml_emits_valid_table_with_pills() throws Exception {
        seedRehearse(1_000L, Outcome.OK);
        seedRehearse(2_000L, Outcome.FAIL);
        DrRehearsalReport.Bundle b = report.build(0L, 100);
        Path f = out.resolve("report.html");
        report.writeHtml(b, f);
        String body = Files.readString(f, StandardCharsets.UTF_8);
        assertTrue(body.startsWith("<!DOCTYPE html>"));
        assertTrue(body.contains("DR rehearsal report"));
        assertTrue(body.contains(">OK<"));
        assertTrue(body.contains(">FAIL<"));
        assertTrue(body.contains("1 ok"));
        assertTrue(body.contains("1 fail"));
    }

    /* ============================ fixtures ============================ */

    private void seedRehearse(long startedAt, Outcome outcome) {
        audit.insert(new OpsAuditRecord(-1, "cx-a", null, null,
                DrRehearsalReport.COMMAND_NAME,
                "{\"mode\":\"REHEARSE\"}",
                "h".repeat(64), outcome, "msg", null,
                startedAt, startedAt + 10, 10L,
                "localhost", "dba", "backup.restore", false, false));
    }

    private void seedOther(long startedAt, String command, Outcome outcome) {
        audit.insert(new OpsAuditRecord(-1, "cx-a", null, null,
                command, "{\"c\":1}", "h".repeat(64), outcome, "msg", null,
                startedAt, startedAt + 10, 10L,
                "localhost", "dba", "cluster.topology", false, false));
    }
}
