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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.4 Q2.4-J.1 — adversarial safety suite. Exercises every {@link Command}
 * variant across every relevant {@link RoleSet} with the kill-switch both
 * engaged and disengaged, asserting the invariants that any destructive
 * dispatch must satisfy regardless of outcome:
 *
 * <ol>
 *   <li>Every call produces an {@code ops_audit} row.</li>
 *   <li>Every row carries a non-null {@code preview_hash}.</li>
 *   <li>Kill-switch engaged ⇒ outcome {@code CANCELLED} and
 *       {@code kill_switch = true}.</li>
 *   <li>Role absent ⇒ outcome {@code FAIL} with {@code role_denied} in
 *       the message; no server call was attempted.</li>
 *   <li>Role present ⇒ dispatch reached (and fails with {@code not_connected}
 *       in this harness, which has no live driver).</li>
 * </ol>
 *
 * <p>With the current 9 destructive command variants × 6 role shapes × 2
 * kill-switch states the suite runs 108 scenarios; well within the spec's
 * "200 command-name / role pairs" headroom.</p>
 */
class DestructiveActionFuzz {

    @TempDir Path dataDir;

    private Database db;
    private EventBus bus;
    private KillSwitch killSwitch;
    private OpsAuditDao audit;
    private RoleCacheDao roleCache;
    private RoleProbeService probe;
    private OpsExecutor executor;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        bus = new EventBus();
        killSwitch = new KillSwitch();
        audit = new OpsAuditDao(db);
        roleCache = new RoleCacheDao(db);
        ConnectionStore store = new ConnectionStore(db);
        ConnectionManager mgr = new ConnectionManager(store, bus, new Crypto());
        probe = new RoleProbeService(mgr::service, roleCache, Clock.systemUTC());
        executor = new OpsExecutor(mgr, audit, bus, killSwitch, probe, Clock.systemUTC());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void every_variant_role_killswitch_combination_honours_invariants() {
        List<Command> commands = commandsUnderTest();
        List<NamedRoleSet> roles = rolesUnderTest();
        List<Boolean> killStates = List.of(true, false);

        int scenarios = 0;
        for (Command cmd : commands) {
            DryRunResult preview = DryRunRenderer.render(cmd);
            assertEquals(64, preview.previewHash().length(),
                    "preview hash must be a 64-char sha256 hex");

            for (NamedRoleSet r : roles) {
                String cx = "cx-" + cmd.name().replaceAll("\\W+", "-") + "-" + r.name;
                roleCache.upsert(cx, r.roles, System.currentTimeMillis());

                for (boolean engaged : killStates) {
                    if (engaged) killSwitch.engage(); else killSwitch.disengage();
                    OpsExecutor.Result result =
                            executor.execute(cx, cmd, preview, false, "harness", "localhost");
                    OpsAuditRecord row = result.audit();
                    scenarios++;

                    // Invariant 1 + 2: audit row + preview hash.
                    assertNotNull(row, "audit row missing for " + cmd.name());
                    assertEquals(preview.previewHash(), row.previewHash(),
                            "preview_hash mismatch for " + cmd.name());

                    if (engaged) {
                        // Invariant 3: kill-switch dominates.
                        assertEquals(Outcome.CANCELLED, result.outcome(),
                                cmd.name() + " under engaged kill-switch must CANCEL");
                        assertTrue(row.killSwitch(),
                                cmd.name() + " audit row must record kill-switch engagement");
                        continue;
                    }

                    if (!cmd.requiredRoles().isEmpty() && !r.roles.allows(cmd)) {
                        // Invariant 4: role denial before dispatch.
                        assertEquals(Outcome.FAIL, result.outcome(),
                                cmd.name() + " without role " + cmd.requiredRoles()
                                        + " must FAIL");
                        assertTrue(result.serverMessage().contains("role_denied"),
                                "expected role_denied message, got " + result.serverMessage());
                        assertNull(row.roleUsed(),
                                "role_used must be null when denial fired");
                    } else {
                        // Invariant 5: dispatch reached. No live driver in this
                        // harness, so it fails with not_connected. The key point
                        // is we got past the role gate.
                        assertTrue(Set.of(Outcome.FAIL).contains(result.outcome()),
                                cmd.name() + ": unexpected outcome " + result.outcome());
                        assertEquals("not_connected", result.serverMessage(),
                                "dispatch should have reached the not_connected step");
                    }
                }
            }
        }
        assertTrue(scenarios >= 100,
                "Sanity: expected >= 100 fuzzed scenarios, got " + scenarios);
    }

    /* ============================= fixtures ============================ */

    private static List<Command> commandsUnderTest() {
        return List.of(
                new Command.StepDown("host1:27017", 60, 10),
                new Command.Freeze("host2:27017", 120),
                new Command.KillOp("host3:27017", 4917L),
                new Command.BalancerStart("cluster-a"),
                new Command.BalancerStop("cluster-a"),
                new Command.BalancerWindow("cluster-a", "00:00", "06:00"),
                new Command.MoveChunk("shop.orders", Map.of("cust", 0), Map.of("cust", 100),
                        "shard1", false, "majority"),
                new Command.AddTagRange("shop.orders", Map.of("region", "eu"),
                        Map.of("region", "us"), "eu-zone"),
                new Command.RemoveTagRange("shop.orders", Map.of("region", "eu"),
                        Map.of("region", "us"))
        );
    }

    private static List<NamedRoleSet> rolesUnderTest() {
        List<NamedRoleSet> out = new ArrayList<>();
        out.add(new NamedRoleSet("empty", RoleSet.EMPTY));
        out.add(new NamedRoleSet("read", new RoleSet(Set.of("read"))));
        out.add(new NamedRoleSet("killAnyCursor", new RoleSet(Set.of("killAnyCursor"))));
        out.add(new NamedRoleSet("clusterManager", new RoleSet(Set.of("clusterManager"))));
        out.add(new NamedRoleSet("root", new RoleSet(Set.of("root"))));
        out.add(new NamedRoleSet("mixed", new RoleSet(Set.of("read", "clusterManager", "killAnyCursor"))));
        return out;
    }

    private record NamedRoleSet(String name, RoleSet roles) {}
}
