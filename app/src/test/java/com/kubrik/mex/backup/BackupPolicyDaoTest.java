package com.kubrik.mex.backup;

import com.kubrik.mex.backup.spec.ArchiveSpec;
import com.kubrik.mex.backup.spec.BackupPolicy;
import com.kubrik.mex.backup.spec.RetentionSpec;
import com.kubrik.mex.backup.spec.Scope;
import com.kubrik.mex.backup.store.BackupPolicyDao;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.5 BKP-POLICY-1..7 — CRUD round-trip for {@link BackupPolicyDao}.
 * Covers all three scope variants so the codec's branches are exercised.
 */
class BackupPolicyDaoTest {

    @TempDir Path dataDir;

    private Database db;
    private BackupPolicyDao dao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        dao = new BackupPolicyDao(db);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void round_trips_whole_cluster_policy() {
        BackupPolicy in = new BackupPolicy(-1, "cx-1", "nightly", true,
                "0 3 * * *", new Scope.WholeCluster(),
                ArchiveSpec.defaults(), RetentionSpec.defaults(), 7L, true,
                1L, 1L);
        BackupPolicy saved = dao.insert(in);
        assertTrue(saved.id() > 0);

        BackupPolicy back = dao.byId(saved.id()).orElseThrow();
        assertEquals("nightly", back.name());
        assertEquals(new Scope.WholeCluster(), back.scope());
        assertTrue(back.archive().gzip());
        assertEquals(6, back.archive().level());
        assertEquals(30, back.retention().maxCount());
        assertTrue(back.includeOplog());
    }

    @Test
    void round_trips_databases_scope() {
        BackupPolicy in = new BackupPolicy(-1, "cx-1", "reports", true, null,
                new Scope.Databases(List.of("reports", "events")),
                ArchiveSpec.defaults(), RetentionSpec.defaults(), 1L, false,
                1L, 1L);
        BackupPolicy saved = dao.insert(in);
        BackupPolicy back = dao.byId(saved.id()).orElseThrow();
        assertEquals(new Scope.Databases(List.of("reports", "events")), back.scope());
        assertNull(back.scheduleCron());
        assertFalse(back.includeOplog());
    }

    @Test
    void round_trips_namespaces_scope_with_non_default_archive_and_retention() {
        BackupPolicy in = new BackupPolicy(-1, "cx-1", "hot-colls", true, "*/15 * * * *",
                new Scope.Namespaces(List.of("shop.orders", "shop.users")),
                new ArchiveSpec(true, 9, "<policy>/tight"),
                new RetentionSpec(100, 7), 3L, true,
                1L, 1L);
        BackupPolicy saved = dao.insert(in);
        BackupPolicy back = dao.byId(saved.id()).orElseThrow();
        assertEquals(new Scope.Namespaces(List.of("shop.orders", "shop.users")), back.scope());
        assertEquals(9, back.archive().level());
        assertEquals("<policy>/tight", back.archive().outputDirTemplate());
        assertEquals(100, back.retention().maxCount());
        assertEquals(7, back.retention().maxAgeDays());
    }

    @Test
    void listForConnection_scopes_per_connection() {
        dao.insert(sample("cx-a", "p1"));
        dao.insert(sample("cx-a", "p2"));
        dao.insert(sample("cx-b", "p3"));
        assertEquals(2, dao.listForConnection("cx-a").size());
        assertEquals(1, dao.listForConnection("cx-b").size());
        // Sorted by name.
        assertEquals("p1", dao.listForConnection("cx-a").get(0).name());
    }

    @Test
    void listEnabled_filters_disabled_rows() {
        BackupPolicy active = dao.insert(sample("cx-a", "nightly"));
        BackupPolicy disabled = dao.insert(new BackupPolicy(-1, "cx-a", "old", false,
                null, new Scope.WholeCluster(),
                ArchiveSpec.defaults(), RetentionSpec.defaults(), 1L, true, 1L, 1L));
        List<BackupPolicy> enabled = dao.listEnabled();
        assertEquals(1, enabled.size());
        assertEquals(active.id(), enabled.get(0).id());
    }

    @Test
    void update_flips_enabled_and_swaps_scope() {
        BackupPolicy saved = dao.insert(sample("cx-a", "p1"));
        BackupPolicy changed = new BackupPolicy(saved.id(), saved.connectionId(),
                saved.name(), false, saved.scheduleCron(),
                new Scope.Databases(List.of("reports")),
                saved.archive(), saved.retention(), saved.sinkId(),
                saved.includeOplog(), saved.createdAt(), 9_999L);
        dao.update(changed);
        BackupPolicy back = dao.byId(saved.id()).orElseThrow();
        assertFalse(back.enabled());
        assertEquals(new Scope.Databases(List.of("reports")), back.scope());
        assertEquals(9_999L, back.updatedAt());
    }

    @Test
    void delete_removes_the_row() {
        BackupPolicy saved = dao.insert(sample("cx-a", "tmp"));
        assertTrue(dao.delete(saved.id()));
        assertTrue(dao.byId(saved.id()).isEmpty());
    }

    private static BackupPolicy sample(String cx, String name) {
        return new BackupPolicy(-1, cx, name, true, "0 3 * * *",
                new Scope.WholeCluster(), ArchiveSpec.defaults(),
                RetentionSpec.defaults(), 1L, true, 1L, 1L);
    }
}
