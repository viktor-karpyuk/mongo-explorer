package com.kubrik.mex.backup;

import com.kubrik.mex.backup.pitr.PitrPlanner;
import com.kubrik.mex.backup.pitr.RestorePlan;
import com.kubrik.mex.backup.store.BackupCatalogDao;
import com.kubrik.mex.backup.store.BackupCatalogRow;
import com.kubrik.mex.backup.store.BackupStatus;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.5 Q2.5-F — planner covers happy path + three refusal modes (too-old,
 * too-new, gap) so the UI can render the specific reason a restore plan
 * can't be built.
 */
class PitrPlannerTest {

    @TempDir Path dataDir;

    private Database db;
    private BackupCatalogDao catalog;
    private PitrPlanner planner;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        catalog = new BackupCatalogDao(db);
        planner = new PitrPlanner(catalog);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void picks_covering_backup_when_target_is_inside_oplog_window() {
        seedBackup("cx-a", 1_000, 2_000, BackupStatus.OK);
        RestorePlan plan = planner.plan("cx-a", 1_500);
        assertTrue(plan.feasible());
        assertEquals(1_500, plan.oplogLimitTs());
    }

    @Test
    void prefers_most_recent_backup_when_multiple_windows_cover_target() {
        seedBackup("cx-a", 1_000, 5_000, BackupStatus.OK);  // older
        BackupCatalogRow newest = seedBackup("cx-a", 4_000, 6_000, BackupStatus.OK);
        RestorePlan plan = planner.plan("cx-a", 4_500);
        assertTrue(plan.feasible());
        assertEquals(newest.id(), plan.source().orElseThrow().id());
    }

    @Test
    void refuses_when_target_predates_every_oplog() {
        seedBackup("cx-a", 1_000, 2_000, BackupStatus.OK);
        RestorePlan plan = planner.plan("cx-a", 500);
        assertFalse(plan.feasible());
        assertTrue(plan.refusal().contains("older than"));
    }

    @Test
    void refuses_when_target_is_in_the_future() {
        seedBackup("cx-a", 1_000, 2_000, BackupStatus.OK);
        RestorePlan plan = planner.plan("cx-a", 9_999);
        assertFalse(plan.feasible());
        assertTrue(plan.refusal().contains("newer than"));
    }

    @Test
    void refuses_when_target_falls_in_gap_between_backups() {
        seedBackup("cx-a", 1_000, 2_000, BackupStatus.OK);
        seedBackup("cx-a", 5_000, 6_000, BackupStatus.OK);
        RestorePlan plan = planner.plan("cx-a", 3_000);
        assertFalse(plan.feasible());
        assertTrue(plan.refusal().contains("gap"));
    }

    @Test
    void ignores_failed_or_missing_oplog_backups() {
        seedBackup("cx-a", 1_000, 5_000, BackupStatus.FAILED);
        RestorePlan plan = planner.plan("cx-a", 3_000);
        assertFalse(plan.feasible(),
                "FAILED rows with oplog windows must not satisfy a plan");
    }

    @Test
    void refuses_for_unknown_connection() {
        RestorePlan plan = planner.plan("cx-nobody", 1L);
        assertFalse(plan.feasible());
        assertTrue(plan.refusal().contains("no backups"));
    }

    private BackupCatalogRow seedBackup(String cx, long firstTs, long lastTs,
                                         BackupStatus status) {
        return catalog.insert(new BackupCatalogRow(-1, null, cx,
                firstTs * 1_000, lastTs * 1_000, status, 1L,
                "p/" + firstTs, "h".repeat(64), 0L, 0L,
                firstTs, lastTs, null, null, null));
    }
}
