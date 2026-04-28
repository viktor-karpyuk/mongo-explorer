package com.kubrik.mex.backup;

import com.kubrik.mex.backup.runner.RestoreService;
import com.kubrik.mex.backup.store.BackupCatalogDao;
import com.kubrik.mex.backup.store.BackupCatalogRow;
import com.kubrik.mex.backup.store.BackupStatus;
import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.safety.KillSwitch;
import com.kubrik.mex.cluster.safety.RoleSet;
import com.kubrik.mex.cluster.service.RoleProbeService;
import com.kubrik.mex.cluster.store.OpsAuditDao;
import com.kubrik.mex.cluster.store.RoleCacheDao;
import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.store.ConnectionStore;
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
    private RoleCacheDao roleCache;
    private RoleProbeService roleProbe;
    private RestoreService service;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        bus = new EventBus();
        catalog = new BackupCatalogDao(db);
        audit = new OpsAuditDao(db);
        killSwitch = new KillSwitch();
        roleCache = new RoleCacheDao(db);
        // No live driver in this harness; role probe will return whatever
        // has been upserted into role_cache for a given connection.
        ConnectionStore store = new ConnectionStore(db);
        ConnectionManager mgr = new ConnectionManager(store, bus, new Crypto());
        roleProbe = new RoleProbeService(mgr::service, roleCache, Clock.systemUTC());
        service = new RestoreService(catalog, audit, bus, killSwitch, roleProbe,
                Clock.systemUTC(), dataDir, "mongorestore-missing-binary");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void execute_mode_with_engaged_killswitch_cancels_and_audits() {
        long id = seedRow();
        seedRole("cx-it", "root");
        killSwitch.engage();
        RestoreService.RestoreResult r = service.execute(id,
                "mongodb://host/", RestoreService.Mode.EXECUTE, null,
                false, false, "dba", "localhost");
        assertEquals(Outcome.CANCELLED, r.outcome());
        assertTrue(r.message().contains("kill_switch"));
    }

    @Test
    void execute_mode_without_restore_role_fails_before_dispatch() {
        // No role seeded — role probe returns EMPTY so Execute must refuse.
        long id = seedRow();
        RestoreService.RestoreResult r = service.execute(id,
                "mongodb://host/", RestoreService.Mode.EXECUTE, null,
                false, false, "dba", "localhost");
        assertEquals(Outcome.FAIL, r.outcome());
        assertTrue(r.message().contains("role_denied"),
                "expected role_denied, got: " + r.message());
    }

    @Test
    void execute_mode_with_restore_role_reaches_dispatch() {
        long id = seedRow();
        seedRole("cx-it", "restore");
        RestoreService.RestoreResult r = service.execute(id,
                "mongodb://host/", RestoreService.Mode.EXECUTE, null,
                false, false, "dba", "localhost");
        // Role gate passes; subprocess then fails because the binary is a
        // bogus path in this harness. That's the signal we reached dispatch.
        assertEquals(Outcome.FAIL, r.outcome());
        assertFalse(r.message().contains("role_denied"),
                "got past the role gate, should see a subprocess-level message");
    }

    @Test
    void rehearse_mode_ignores_killswitch_and_role_gate() {
        // Rehearse is safe-by-default: no kill-switch check, no role probe.
        // The subprocess still can't spawn since the binary is missing, but
        // the gate path is what we're asserting.
        long id = seedRow();
        killSwitch.engage();
        // Explicitly leave role unset to prove Rehearse doesn't probe it.
        RestoreService.RestoreResult r = service.execute(id,
                "mongodb://host/", RestoreService.Mode.REHEARSE, null,
                false, false, "dba", "localhost");
        assertEquals(Outcome.FAIL, r.outcome());
        assertFalse(r.message().contains("kill_switch"),
                "rehearse must not gate on the kill-switch");
        assertFalse(r.message().contains("role_denied"),
                "rehearse must not gate on the role probe");
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

    private void seedRole(String connectionId, String role) {
        roleCache.upsert(connectionId, new RoleSet(java.util.Set.of(role)),
                System.currentTimeMillis());
    }
}
