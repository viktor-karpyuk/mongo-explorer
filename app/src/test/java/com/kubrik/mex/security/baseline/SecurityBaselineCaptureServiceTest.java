package com.kubrik.mex.security.baseline;

import com.kubrik.mex.security.access.AuthenticationRestriction;
import com.kubrik.mex.security.access.Privilege;
import com.kubrik.mex.security.access.RoleBinding;
import com.kubrik.mex.security.access.RoleRecord;
import com.kubrik.mex.security.access.UserRecord;
import com.kubrik.mex.security.access.UsersRolesFetcher;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-B4 — capture-and-persist round-trip using a fixture snapshot.
 * Exercises payload shape, determinism (re-capture of an equal snapshot
 * produces the same SHA-256), and DAO wiring. A live MongoDB is not
 * needed — persist(…) is the injection point that accepts a pre-built
 * Snapshot.
 */
class SecurityBaselineCaptureServiceTest {

    @TempDir Path home;
    private Database db;
    private SecurityBaselineDao dao;
    private SecurityBaselineCaptureService service;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", home.toString());
        db = new Database();
        dao = new SecurityBaselineDao(db);
        service = new SecurityBaselineCaptureService(
                new UsersRolesFetcher(), dao,
                Clock.fixed(Instant.parse("2026-04-21T17:00:00Z"), ZoneOffset.UTC));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void persist_writes_row_and_returns_baseline_id_plus_sha256() {
        SecurityBaselineCaptureService.Result r =
                service.persist("cx-a", "dba", "pre-release snapshot", fixture());
        assertTrue(r.baselineId() > 0);
        assertEquals(64, r.sha256().length());
        assertEquals(2, r.userCount());
        assertEquals(2, r.roleCount());

        SecurityBaselineDao.Row row = dao.byId(r.baselineId()).orElseThrow();
        assertEquals("cx-a", row.connectionId());
        assertEquals("dba", row.capturedBy());
        assertEquals("pre-release snapshot", row.notes());
        assertEquals(r.sha256(), row.sha256());
    }

    @Test
    void equal_snapshots_hash_equally_across_captures() {
        SecurityBaselineCaptureService.Result r1 =
                service.persist("cx-a", "dba", "", fixture());
        SecurityBaselineCaptureService.Result r2 =
                service.persist("cx-a", "dba", "", fixture());

        // Two captures of structurally-equal snapshots must produce
        // the same SHA-256 — this is what the drift engine's fast path
        // relies on. The rows are distinct (different ids) but the
        // digest matches.
        assertEquals(r1.sha256(), r2.sha256());
        assertNotEquals(r1.baselineId(), r2.baselineId());
    }

    @Test
    void null_snapshot_still_persists_a_row_with_empty_payload() {
        // Defensive: if the fetcher's admin call failed and returned null,
        // we still write a baseline so the operator can tell *something*
        // was attempted. Empty payload → the drift diff will show every
        // user and role appearing on the next capture.
        SecurityBaselineCaptureService.Result r =
                service.persist("cx-a", "dba", "admin unreachable", null);
        assertTrue(r.baselineId() > 0);
        assertEquals(0, r.userCount());
        assertEquals(0, r.roleCount());
    }

    @Test
    void payload_shape_includes_expected_user_and_role_fields() {
        SecurityBaselineCaptureService.Result r =
                service.persist("cx-a", "dba", "", fixture());
        SecurityBaselineDao.Row row = dao.byId(r.baselineId()).orElseThrow();
        String json = row.snapshotJson();

        // Spot-check presence of fields the drift engine pathes into.
        assertTrue(json.contains("\"users\":"), "payload should include users map");
        assertTrue(json.contains("\"roles\":"), "payload should include roles map");
        assertTrue(json.contains("\"dba@admin\""), "user map keyed on user@db");
        assertTrue(json.contains("\"appOps@admin\""), "role map keyed on role@db");
        assertTrue(json.contains("\"builtin\":false") || json.contains("\"builtin\":true"),
                "role entries carry builtin flag");
    }

    /* ============================== fixture ============================== */

    private static UsersRolesFetcher.Snapshot fixture() {
        RoleRecord appOps = new RoleRecord("admin", "appOps",
                List.of(),
                List.of(new Privilege(Privilege.Resource.ofNamespace("shop", ""),
                        List.of("find"))),
                List.of(new RoleBinding("read", "admin")),
                List.of(), false);
        RoleRecord audit = new RoleRecord("admin", "auditReader",
                List.of(), List.of(), List.of(), List.of(), false);

        UserRecord dba = new UserRecord("admin", "dba",
                List.of(new RoleBinding("root", "admin")),
                List.of(new Privilege(Privilege.Resource.ofAnyResource(), List.of("find"))),
                List.of());
        UserRecord ops = new UserRecord("shop", "ops",
                List.of(new RoleBinding("appOps", "admin")),
                List.of(new Privilege(Privilege.Resource.ofNamespace("shop", ""),
                        List.of("find"))),
                List.of(AuthenticationRestriction.empty()));
        return new UsersRolesFetcher.Snapshot(List.of(dba, ops), List.of(appOps, audit));
    }
}
