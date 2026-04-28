package com.kubrik.mex.security.access;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-B1 — pure parser tests: fixture reply documents fed through
 * {@link UsersRolesFetcher#parse} exercise the full cross-product of
 * privilege resource shapes and role-inheritance entries without a
 * live MongoDB.
 */
class UsersRolesFetcherTest {

    @Test
    void parses_user_with_inherited_privilege_on_db_collection_resource() {
        Document usersReply = new Document("users", List.of(new Document()
                .append("db", "admin")
                .append("user", "dba")
                .append("roles", List.of(new Document("role", "root").append("db", "admin")))
                .append("inheritedPrivileges", List.of(new Document()
                        .append("resource", new Document("db", "shop").append("collection", "orders"))
                        .append("actions", List.of("find", "insert"))))));

        var snap = UsersRolesFetcher.parse(usersReply, new Document("roles", List.of()));

        assertEquals(1, snap.users().size());
        UserRecord u = snap.users().get(0);
        assertEquals("dba@admin", u.fullyQualified());
        assertEquals(List.of(new RoleBinding("root", "admin")), u.roleBindings());
        assertEquals(1, u.inheritedPrivileges().size());
        Privilege p = u.inheritedPrivileges().get(0);
        assertEquals("shop.orders", p.resource().render());
        // Actions must be sorted — canonical-JSON stability.
        assertEquals(List.of("find", "insert"), p.actions());
    }

    @Test
    void parses_cluster_resource_and_anyResource_privileges() {
        Document usersReply = new Document("users", List.of(new Document()
                .append("db", "admin")
                .append("user", "metrics")
                .append("roles", List.of())
                .append("inheritedPrivileges", List.of(
                        new Document()
                                .append("resource", new Document("cluster", true))
                                .append("actions", List.of("serverStatus", "replSetGetStatus")),
                        new Document()
                                .append("resource", new Document("anyResource", true))
                                .append("actions", List.of("find"))))));

        var snap = UsersRolesFetcher.parse(usersReply, new Document("roles", List.of()));
        UserRecord u = snap.users().get(0);

        assertEquals("cluster", u.inheritedPrivileges().get(0).resource().render());
        assertEquals("<any>",   u.inheritedPrivileges().get(1).resource().render());
    }

    @Test
    void parses_role_with_direct_and_inherited_tiers() {
        Document rolesReply = new Document("roles", List.of(new Document()
                .append("db", "admin")
                .append("role", "appOps")
                .append("isBuiltin", false)
                .append("roles", List.of(new Document("role", "readAnyDatabase").append("db", "admin")))
                .append("inheritedRoles", List.of(
                        new Document("role", "readAnyDatabase").append("db", "admin"),
                        new Document("role", "clusterMonitor").append("db", "admin")))
                .append("privileges", List.of(new Document()
                        .append("resource", new Document("db", "").append("collection", ""))
                        .append("actions", List.of("find"))))
                .append("inheritedPrivileges", List.of(
                        new Document()
                                .append("resource", new Document("db", "shop").append("collection", ""))
                                .append("actions", List.of("find"))))));

        var snap = UsersRolesFetcher.parse(new Document("users", List.of()), rolesReply);

        assertEquals(1, snap.roles().size());
        RoleRecord r = snap.roles().get(0);
        assertFalse(r.builtin());
        assertEquals("appOps@admin", r.fullyQualified());
        assertEquals(List.of(
                new RoleBinding("readAnyDatabase", "admin")), r.directRoles());
        assertEquals(2, r.inheritedRoles().size());
        assertEquals("shop.*", r.inheritedPrivileges().get(0).resource().render());
    }

    @Test
    void parses_authentication_restrictions_without_leaking_secrets() {
        Document usersReply = new Document("users", List.of(new Document()
                .append("db", "admin")
                .append("user", "auditor")
                .append("roles", List.of())
                .append("inheritedPrivileges", List.of())
                .append("authenticationRestrictions", List.of(
                        new Document()
                                .append("clientSource", List.of("10.0.0.0/24"))
                                .append("serverAddress", List.of("10.0.1.5"))))));

        var snap = UsersRolesFetcher.parse(usersReply, new Document("roles", List.of()));
        UserRecord u = snap.users().get(0);

        assertEquals(1, u.authenticationRestrictions().size());
        AuthenticationRestriction r = u.authenticationRestrictions().get(0);
        assertEquals(List.of("10.0.0.0/24"), r.clientSource());
        assertEquals(List.of("10.0.1.5"),     r.serverAddress());
    }

    @Test
    void missing_reply_becomes_empty_snapshot() {
        var snap = UsersRolesFetcher.parse(null, null);
        assertTrue(snap.users().isEmpty());
        assertTrue(snap.roles().isEmpty());
    }

    @Test
    void isBuiltin_flag_is_surfaced() {
        Document rolesReply = new Document("roles", List.of(
                new Document()
                        .append("db", "admin").append("role", "root").append("isBuiltin", true)
                        .append("roles", List.of()).append("inheritedRoles", List.of())
                        .append("privileges", List.of()).append("inheritedPrivileges", List.of()),
                new Document()
                        .append("db", "admin").append("role", "custom").append("isBuiltin", false)
                        .append("roles", List.of()).append("inheritedRoles", List.of())
                        .append("privileges", List.of()).append("inheritedPrivileges", List.of())));

        var snap = UsersRolesFetcher.parse(new Document("users", List.of()), rolesReply);

        assertTrue(snap.roles().stream().anyMatch(r -> r.builtin() && r.role().equals("root")));
        assertTrue(snap.roles().stream().anyMatch(r -> !r.builtin() && r.role().equals("custom")));
    }
}
