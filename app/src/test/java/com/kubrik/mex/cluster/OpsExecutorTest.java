package com.kubrik.mex.cluster;

import com.kubrik.mex.cluster.audit.OpsAuditRecord;
import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.dryrun.DryRunRenderer;
import com.kubrik.mex.cluster.safety.Command;
import com.kubrik.mex.cluster.safety.DryRunResult;
import com.kubrik.mex.cluster.safety.KillSwitch;
import com.kubrik.mex.cluster.safety.RoleSet;
import com.kubrik.mex.cluster.service.OpsExecutor;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.4 SAFE-OPS-* / AUD-* — gate coverage for {@link OpsExecutor}. Verifies
 * that a disengaged kill-switch + absent role produce the documented
 * {@link Outcome} and audit row shape without needing a live MongoDB dispatch.
 * The happy-path round-trip is covered by {@code KillOpIT} (runs against
 * testcontainers).
 */
class OpsExecutorTest {

    @TempDir Path dataDir;

    private Database db;
    private EventBus bus;
    private KillSwitch killSwitch;
    private OpsAuditDao audit;
    private RoleCacheDao roleCache;
    private RoleProbeService probe;
    private ConnectionManager connManager;
    private OpsExecutor executor;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        bus = new EventBus();
        killSwitch = new KillSwitch();
        audit = new OpsAuditDao(db);
        roleCache = new RoleCacheDao(db);

        // No live driver — service lookup returns null. Role probe therefore
        // returns whatever we pre-seed into the role_cache table for each
        // connection-id before calling execute.
        ConnectionStore store = new ConnectionStore(db);
        connManager = new ConnectionManager(store, bus, new Crypto());
        probe = new RoleProbeService(connManager::service, roleCache, Clock.systemUTC());
        executor = new OpsExecutor(connManager, audit, bus, killSwitch, probe, Clock.systemUTC());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void killSwitch_engaged_cancels_with_audit_row() {
        killSwitch.engage();
        Command.KillOp cmd = new Command.KillOp("h1:27017", 4917L);
        DryRunResult preview = DryRunRenderer.render(cmd);

        AtomicReference<OpsAuditRecord> seen = new AtomicReference<>();
        bus.onOpsAudit(seen::set);

        OpsExecutor.Result r = executor.execute("cx-1", cmd, preview, false, "u", "h");
        assertEquals(Outcome.CANCELLED, r.outcome());
        assertNotNull(r.audit(), "audit row persisted even on cancel");
        assertTrue(r.audit().killSwitch(), "kill_switch flag captured");
        assertEquals(preview.previewHash(), r.audit().previewHash());
        assertEquals(preview.previewHash(), seen.get().previewHash(), "event bus received the same row");
    }

    @Test
    void missing_role_fails_with_denial_message() {
        // Empty role set via explicit upsert — avoids the probe call's live
        // Mongo round-trip (which would fail here since the service lookup
        // returns null).
        roleCache.upsert("cx-1", RoleSet.EMPTY, System.currentTimeMillis());
        Command.KillOp cmd = new Command.KillOp("h1:27017", 4917L);
        DryRunResult preview = DryRunRenderer.render(cmd);

        OpsExecutor.Result r = executor.execute("cx-1", cmd, preview, true, "u", "h");
        assertEquals(Outcome.FAIL, r.outcome());
        assertTrue(r.serverMessage().contains("role_denied"));
        assertTrue(r.audit().paste(), "paste flag carried into audit row");
    }

    @Test
    void role_present_but_connection_missing_is_FAIL_not_crash() {
        roleCache.upsert("cx-missing", new RoleSet(java.util.Set.of("root")),
                System.currentTimeMillis());
        Command.KillOp cmd = new Command.KillOp("h1:27017", 4917L);
        DryRunResult preview = DryRunRenderer.render(cmd);

        OpsExecutor.Result r = executor.execute("cx-missing", cmd, preview, false, "u", "h");
        assertEquals(Outcome.FAIL, r.outcome());
        assertEquals("not_connected", r.serverMessage());
    }

    @Test
    void balancer_start_and_stop_pass_the_role_gate() {
        // Give the connection the needed role, so the gate passes. The command
        // still fails at dispatch (no live driver), but the point is that the
        // gate-reached-dispatch path works for the balancer variants.
        roleCache.upsert("cx-b", new RoleSet(java.util.Set.of("clusterManager")),
                System.currentTimeMillis());
        Command.BalancerStart start = new Command.BalancerStart("cx-b");
        DryRunResult preview = DryRunRenderer.render(start);
        OpsExecutor.Result r = executor.execute("cx-b", start, preview, false, "u", "h");
        assertEquals(Outcome.FAIL, r.outcome());
        assertEquals("not_connected", r.serverMessage(),
                "balancer commands must reach the dispatch step (not be rejected earlier)");

        Command.BalancerStop stop = new Command.BalancerStop("cx-b");
        DryRunResult preview2 = DryRunRenderer.render(stop);
        OpsExecutor.Result r2 = executor.execute("cx-b", stop, preview2, false, "u", "h");
        assertEquals(Outcome.FAIL, r2.outcome());
        assertEquals("not_connected", r2.serverMessage());
    }
}
