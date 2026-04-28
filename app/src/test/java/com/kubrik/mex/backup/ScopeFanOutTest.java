package com.kubrik.mex.backup;

import com.kubrik.mex.backup.spec.Scope;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-L6 — scope fan-out contract used by BackupRunner to decide
 * how many mongodump invocations a policy needs. Single-entry scopes
 * stay as-is (back-compat with v2.5 single-run behaviour); multi-entry
 * variants expand to one Scope per entry.
 */
class ScopeFanOutTest {

    @Test
    void whole_cluster_is_a_single_run() {
        List<Scope> fan = Scope.fanOut(new Scope.WholeCluster());
        assertEquals(1, fan.size());
        assertInstanceOf(Scope.WholeCluster.class, fan.get(0));
    }

    @Test
    void single_db_scope_stays_single_run() {
        List<Scope> fan = Scope.fanOut(new Scope.Databases(List.of("shop")));
        assertEquals(1, fan.size());
        assertEquals(List.of("shop"),
                ((Scope.Databases) fan.get(0)).names());
    }

    @Test
    void multiple_dbs_expand_to_one_scope_per_db() {
        List<Scope> fan = Scope.fanOut(
                new Scope.Databases(List.of("shop", "metrics", "audit")));
        assertEquals(3, fan.size());
        assertEquals(List.of("shop"),    ((Scope.Databases) fan.get(0)).names());
        assertEquals(List.of("metrics"), ((Scope.Databases) fan.get(1)).names());
        assertEquals(List.of("audit"),   ((Scope.Databases) fan.get(2)).names());
    }

    @Test
    void single_namespace_stays_single_run() {
        List<Scope> fan = Scope.fanOut(
                new Scope.Namespaces(List.of("shop.orders")));
        assertEquals(1, fan.size());
        assertEquals(List.of("shop.orders"),
                ((Scope.Namespaces) fan.get(0)).namespaces());
    }

    @Test
    void multiple_namespaces_expand_to_one_scope_per_namespace() {
        List<Scope> fan = Scope.fanOut(new Scope.Namespaces(
                List.of("shop.orders", "shop.users", "metrics.daily")));
        assertEquals(3, fan.size());
        assertEquals(List.of("shop.orders"),
                ((Scope.Namespaces) fan.get(0)).namespaces());
        assertEquals(List.of("shop.users"),
                ((Scope.Namespaces) fan.get(1)).namespaces());
        assertEquals(List.of("metrics.daily"),
                ((Scope.Namespaces) fan.get(2)).namespaces());
    }

    @Test
    void fanned_out_entries_remain_valid_single_entry_scopes() {
        // Each expanded scope must still satisfy the record's canonical
        // constructor invariants — empty lists would throw, so a
        // malformed expansion would fail-fast here rather than silently
        // produce nonsense argv.
        List<Scope> fan = Scope.fanOut(new Scope.Namespaces(
                List.of("a.b", "c.d")));
        for (Scope s : fan) {
            Scope.Namespaces ns = (Scope.Namespaces) s;
            assertEquals(1, ns.namespaces().size());
            assertTrue(ns.namespaces().get(0).contains("."));
        }
    }
}
