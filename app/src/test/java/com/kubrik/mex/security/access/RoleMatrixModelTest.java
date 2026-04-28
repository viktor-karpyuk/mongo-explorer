package com.kubrik.mex.security.access;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-B2 — headless matrix model. Filter combinations behave as
 * expected, effective-role computation follows role inheritance, and
 * built-in roles are hidden by default.
 */
class RoleMatrixModelTest {

    private RoleMatrixModel model;

    @BeforeEach
    void setUp() {
        model = new RoleMatrixModel();
        model.load(fixture());
    }

    @Test
    void default_filtered_roles_hides_builtins() {
        List<RoleRecord> roles = model.filteredRoles();
        assertTrue(roles.stream().noneMatch(RoleRecord::builtin),
                "default matrix hides MongoDB built-in roles");
        assertTrue(roles.stream().anyMatch(r -> r.role().equals("appOps")));
    }

    @Test
    void include_builtin_flag_surfaces_them() {
        model.includeBuiltinRolesProperty().set(true);
        List<RoleRecord> roles = model.filteredRoles();
        assertTrue(roles.stream().anyMatch(r -> r.role().equals("read")),
                "built-in 'read' role appears when the flag is on");
    }

    @Test
    void text_filter_matches_user_identity_and_bound_role_names() {
        // Match by user identity
        model.textFilterProperty().set("dba");
        List<UserRecord> usersA = model.filteredUsers();
        assertEquals(1, usersA.size());
        assertEquals("dba@admin", usersA.get(0).fullyQualified());

        // Match a user by its bound role name ("readOnlyReports" → no such
        // user identity, but 'reports' has a role bound to it).
        model.textFilterProperty().set("appops");
        List<UserRecord> usersB = model.filteredUsers();
        assertTrue(usersB.stream().anyMatch(u -> u.fullyQualified().equals("ops@shop")));
    }

    @Test
    void effective_roles_walk_inheritance_chain() {
        UserRecord ops = model.filteredUsers().stream()
                .filter(u -> u.user().equals("ops")).findFirst().orElseThrow();
        List<RoleBinding> effective = model.effectiveRoles(ops);

        // ops has appOps directly; appOps inherits read@admin and
        // clusterMonitor@admin. All three should appear.
        assertTrue(effective.stream().anyMatch(b -> b.role().equals("appOps")));
        assertTrue(effective.stream().anyMatch(b -> b.role().equals("read")));
        assertTrue(effective.stream().anyMatch(b -> b.role().equals("clusterMonitor")));
    }

    @Test
    void effective_privileges_are_sorted_by_rendered_resource() {
        UserRecord ops = model.filteredUsers().stream()
                .filter(u -> u.user().equals("ops")).findFirst().orElseThrow();
        List<Privilege> privs = model.effectivePrivileges(ops);

        // cluster → shop.* → shop.orders (alphabetical render keys).
        assertEquals("cluster",     privs.get(0).resource().render());
        assertEquals("shop.*",      privs.get(1).resource().render());
        assertEquals("shop.orders", privs.get(2).resource().render());
    }

    @Test
    void users_by_db_counts_per_auth_db() {
        var counts = model.usersByDb();
        assertEquals(1L, counts.get("admin"));
        assertEquals(1L, counts.get("shop"));
    }

    @Test
    void load_null_clears_the_model() {
        model.load(null);
        assertTrue(model.filteredUsers().isEmpty());
        assertTrue(model.filteredRoles().isEmpty());
    }

    /* ============================== fixture ============================== */

    private static UsersRolesFetcher.Snapshot fixture() {
        RoleRecord readBuiltin = new RoleRecord("admin", "read",
                List.of(), List.of(), List.of(), List.of(), true);
        RoleRecord clusterMonitorBuiltin = new RoleRecord("admin", "clusterMonitor",
                List.of(), List.of(), List.of(), List.of(), true);
        RoleRecord appOps = new RoleRecord("admin", "appOps",
                List.of(),
                List.of(new Privilege(Privilege.Resource.ofNamespace("shop", ""),
                        List.of("find"))),
                List.of(
                        new RoleBinding("read", "admin"),
                        new RoleBinding("clusterMonitor", "admin")),
                List.of(), false);

        UserRecord dba = new UserRecord("admin", "dba",
                List.of(new RoleBinding("root", "admin")),
                List.of(new Privilege(Privilege.Resource.ofAnyResource(), List.of("find"))),
                List.of());
        UserRecord ops = new UserRecord("shop", "ops",
                List.of(new RoleBinding("appOps", "admin")),
                List.of(
                        new Privilege(Privilege.Resource.ofCluster(), List.of("serverStatus")),
                        new Privilege(Privilege.Resource.ofNamespace("shop", ""), List.of("find")),
                        new Privilege(Privilege.Resource.ofNamespace("shop", "orders"),
                                List.of("insert"))),
                List.of());

        return new UsersRolesFetcher.Snapshot(
                List.of(dba, ops),
                List.of(readBuiltin, clusterMonitorBuiltin, appOps));
    }
}
