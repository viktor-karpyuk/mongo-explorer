package com.kubrik.mex.backup;

import com.kubrik.mex.backup.runner.RestoreService;
import com.kubrik.mex.backup.store.BackupCatalogDao;
import com.kubrik.mex.backup.store.BackupCatalogRow;
import com.kubrik.mex.backup.store.BackupStatus;
import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.safety.KillSwitch;
import com.kubrik.mex.cluster.store.OpsAuditDao;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.5 Q2.5-E — gate coverage for {@link RestoreService}: Execute mode with
 * engaged kill-switch cancels before dispatch; missing catalog row fails
 * fast. Happy-path mongorestore round-trip lives in the shim-backed
 * RestoreServiceIT (deferred — the mongorestore shim needs more plumbing
 * than mongodump's since restore reads the backup tree).
 */
class RestoreServiceTest {

    @TempDir Path dataDir;

    private Database db;
    private EventBus bus;
    private BackupCatalogDao catalog;
    private OpsAuditDao audit;
    private KillSwitch killSwitch;
    private RestoreService service;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        bus = new EventBus();
        catalog = new BackupCatalogDao(db);
        audit = new OpsAuditDao(db);
        killSwitch = new KillSwitch();
        service = new RestoreService(catalog, audit, bus, killSwitch,
                Clock.systemUTC(), dataDir, "mongorestore-missing-binary");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void execute_mode_with_engaged_killswitch_cancels_and_audits() {
        long id = seedRow();
        killSwitch.engage();
        RestoreService.RestoreResult r = service.execute(id,
                "mongodb://host/", RestoreService.Mode.EXECUTE, null,
                false, false, "dba", "localhost");
        assertEquals(Outcome.CANCELLED, r.outcome());
        assertTrue(r.message().contains("kill_switch"));
    }

    @Test
    void rehearse_mode_ignores_killswitch() {
        // Rehearse is safe-by-default and never checks the kill-switch; the
        // subprocess still can't spawn since the binary is missing, but the
        // gate path is what we're asserting.
        long id = seedRow();
        killSwitch.engage();
        RestoreService.RestoreResult r = service.execute(id,
                "mongodb://host/", RestoreService.Mode.REHEARSE, null,
                false, false, "dba", "localhost");
        // Expect FAIL (binary missing), not CANCELLED.
        assertEquals(Outcome.FAIL, r.outcome());
    }

    @Test
    void missing_catalog_row_fails_fast() {
        RestoreService.RestoreResult r = service.execute(9999L,
                "mongodb://host/", RestoreService.Mode.EXECUTE, null,
                false, false, "dba", "localhost");
        assertEquals(Outcome.FAIL, r.outcome());
        assertTrue(r.message().contains("not found"));
    }

    private long seedRow() {
        return catalog.insert(new BackupCatalogRow(-1, null, "cx-it",
                1_000L, 2_000L, BackupStatus.OK, 7L, "backups/nightly",
                "h".repeat(64), 0L, 0L, null, null, null, null, null)).id();
    }
}
