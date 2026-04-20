package com.kubrik.mex.cluster;

import com.kubrik.mex.cluster.audit.AuditJanitor;
import com.kubrik.mex.cluster.audit.OpsAuditRecord;
import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.store.OpsAuditDao;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.4 AUD-RET-1..2 — retention sweep preserves {@code outcome = FAIL} rows
 * and rows produced under {@code root} / {@code clusterAdmin}, regardless of
 * age. Only plain {@code OK} rows older than the retention window are
 * auto-purged; the user can clear the exempt ones explicitly via the pane.
 */
class AuditJanitorTest {

    @TempDir Path dataDir;

    private Database db;
    private OpsAuditDao dao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        dao = new OpsAuditDao(db);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void purges_old_ok_rows_but_keeps_fail_and_elevated_roles() {
        long now = 1_800_000_000_000L;  // deterministic "now" in ms
        long oldTs = now - 200L * 86_400_000L;  // 200 days old

        // Old OK row — should be deleted.
        dao.insert(row(oldTs, "killOp", Outcome.OK, "app-rw", "cx-a"));
        // Old FAIL row — exempt.
        dao.insert(row(oldTs, "killOp", Outcome.FAIL, "app-rw", "cx-a"));
        // Old OK row by root — exempt.
        dao.insert(row(oldTs, "killOp", Outcome.OK, "root", "cx-a"));
        // Old OK row by clusterAdmin — exempt.
        dao.insert(row(oldTs, "replSetStepDown", Outcome.OK, "clusterAdmin", "cx-a"));
        // Fresh OK row — inside retention window, kept.
        dao.insert(row(now - 86_400_000L, "killOp", Outcome.OK, "app-rw", "cx-a"));

        AuditJanitor janitor = new AuditJanitor(db,
                Clock.fixed(Instant.ofEpochMilli(now), ZoneId.systemDefault()), 180);
        int deleted = janitor.sweep();
        assertEquals(1, deleted, "only the old OK / non-elevated row purged");

        // Remaining four rows should all still be there.
        assertEquals(4, dao.listForConnection("cx-a", 100).size());
    }

    private static OpsAuditRecord row(long startedAt, String cmd, Outcome outcome,
                                      String roleUsed, String cx) {
        return new OpsAuditRecord(
                -1L, cx, null, null, cmd, "{\"" + cmd + "\":1}",
                "h".repeat(64), outcome, "msg", roleUsed,
                startedAt, startedAt + 10, 10L,
                "localhost", "dba", "cluster.topology", false, false);
    }
}
