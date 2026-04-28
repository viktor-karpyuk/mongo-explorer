package com.kubrik.mex.security.drift;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-D1 — drift engine pinning: path format, kind classification
 * (ADDED / REMOVED / CHANGED), list-positional semantics, and section
 * routing for the pane's grouping.
 */
class DriftDiffEngineTest {

    @Test
    void identical_payloads_produce_no_findings() {
        Map<String, Object> same = Map.of("users", Map.of("dba@admin",
                Map.of("roles", List.of(Map.of("role", "root", "db", "admin")))));
        assertTrue(DriftDiffEngine.diff(same, same).isEmpty());
    }

    @Test
    void adding_a_user_surfaces_one_ADDED_finding_under_users() {
        Map<String, Object> before = Map.of("users", Map.of(), "roles", Map.of());
        Map<String, Object> after = Map.of("users", Map.of("ops@shop",
                Map.of("user", "ops", "db", "shop")), "roles", Map.of());

        List<DriftFinding> findings = DriftDiffEngine.diff(before, after);

        assertEquals(1, findings.size());
        DriftFinding f = findings.get(0);
        assertEquals("users.ops@shop", f.path());
        assertEquals(DriftFinding.Kind.ADDED, f.kind());
        assertEquals("users", f.section());
        assertNull(f.before());
        assertNotNull(f.after());
    }

    @Test
    void removing_a_role_surfaces_one_REMOVED_finding() {
        Map<String, Object> before = Map.of("roles", Map.of("audit@admin",
                Map.of("role", "audit", "db", "admin")));
        Map<String, Object> after = Map.of("roles", Map.of());

        List<DriftFinding> findings = DriftDiffEngine.diff(before, after);

        assertEquals(1, findings.size());
        DriftFinding f = findings.get(0);
        assertEquals("roles.audit@admin", f.path());
        assertEquals(DriftFinding.Kind.REMOVED, f.kind());
        assertEquals("roles", f.section());
    }

    @Test
    void changing_a_scalar_produces_one_CHANGED_finding_with_before_and_after() {
        Map<String, Object> before = Map.of("users", Map.of("dba@admin",
                Map.of("user", "dba", "db", "admin", "builtin", false)));
        Map<String, Object> after = Map.of("users", Map.of("dba@admin",
                Map.of("user", "dba", "db", "admin", "builtin", true)));

        List<DriftFinding> findings = DriftDiffEngine.diff(before, after);

        assertEquals(1, findings.size());
        DriftFinding f = findings.get(0);
        assertEquals("users.dba@admin.builtin", f.path());
        assertEquals(DriftFinding.Kind.CHANGED, f.kind());
        assertEquals("false", f.before());
        assertEquals("true", f.after());
    }

    @Test
    void list_entries_use_bracketed_index_in_the_path() {
        Map<String, Object> before = Map.of("users", Map.of("dba@admin",
                Map.of("roleBindings",
                        List.of(Map.of("role", "root", "db", "admin")))));
        Map<String, Object> after = Map.of("users", Map.of("dba@admin",
                Map.of("roleBindings",
                        List.of(Map.of("role", "root", "db", "admin"),
                                Map.of("role", "clusterAdmin", "db", "admin")))));

        List<DriftFinding> findings = DriftDiffEngine.diff(before, after);

        assertEquals(1, findings.size());
        DriftFinding f = findings.get(0);
        assertEquals("users.dba@admin.roleBindings[1]", f.path());
        assertEquals(DriftFinding.Kind.ADDED, f.kind());
    }

    @Test
    void role_field_inside_a_list_entry_diffs_into_nested_path() {
        Map<String, Object> before = Map.of("users", Map.of("ops@shop",
                Map.of("roleBindings",
                        List.of(Map.of("role", "appOps", "db", "admin")))));
        Map<String, Object> after = Map.of("users", Map.of("ops@shop",
                Map.of("roleBindings",
                        List.of(Map.of("role", "appOpsV2", "db", "admin")))));

        List<DriftFinding> findings = DriftDiffEngine.diff(before, after);

        assertEquals(1, findings.size());
        DriftFinding f = findings.get(0);
        assertEquals("users.ops@shop.roleBindings[0].role", f.path());
        assertEquals(DriftFinding.Kind.CHANGED, f.kind());
        assertEquals("appOps", f.before());
        assertEquals("appOpsV2", f.after());
    }

    @Test
    void section_tracks_the_top_level_key_for_deep_nested_findings() {
        Map<String, Object> before = Map.of(
                "users", Map.of("dba@admin", Map.of("db", "admin")),
                "roles", Map.of("appOps@admin", Map.of("builtin", false)));
        Map<String, Object> after = Map.of(
                "users", Map.of("dba@admin", Map.of("db", "admin2")),
                "roles", Map.of("appOps@admin", Map.of("builtin", true)));

        List<DriftFinding> findings = DriftDiffEngine.diff(before, after);

        assertEquals(2, findings.size());
        assertTrue(findings.stream().anyMatch(
                f -> f.path().startsWith("users.") && f.section().equals("users")));
        assertTrue(findings.stream().anyMatch(
                f -> f.path().startsWith("roles.") && f.section().equals("roles")));
    }

    @Test
    void null_baselines_are_treated_as_empty() {
        Map<String, Object> after = Map.of("users", Map.of("dba@admin",
                Map.of("db", "admin")));

        List<DriftFinding> findings = DriftDiffEngine.diff(null, after);

        assertEquals(1, findings.size());
        assertEquals(DriftFinding.Kind.ADDED, findings.get(0).kind());
    }

    @Test
    void list_removal_produces_REMOVED_findings_for_the_missing_tail() {
        Map<String, Object> before = Map.of("users", Map.of("dba@admin",
                Map.of("actions",
                        List.of("find", "insert", "remove"))));
        Map<String, Object> after = Map.of("users", Map.of("dba@admin",
                Map.of("actions",
                        List.of("find"))));

        List<DriftFinding> findings = DriftDiffEngine.diff(before, after);

        assertEquals(2, findings.size());
        assertTrue(findings.stream().allMatch(f -> f.kind() == DriftFinding.Kind.REMOVED));
        assertTrue(findings.stream().anyMatch(f -> f.path().endsWith("actions[1]")));
        assertTrue(findings.stream().anyMatch(f -> f.path().endsWith("actions[2]")));
    }
}
