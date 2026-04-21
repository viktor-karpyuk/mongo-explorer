package com.kubrik.mex.backup;

import com.kubrik.mex.backup.store.BackupCatalogDao;
import com.kubrik.mex.backup.store.BackupCatalogRow;
import com.kubrik.mex.backup.store.BackupFileDao;
import com.kubrik.mex.backup.store.BackupFileRow;
import com.kubrik.mex.backup.store.BackupStatus;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.5 BKP-RUN-1..8 — catalog + file-inventory round-trip. Covers the two
 * lifecycle shapes: (1) insert → finalise → verify, and (2) cascade delete
 * of file rows when the catalog row is purged (Q2.5-D retention janitor).
 */
class BackupCatalogDaoTest {

    @TempDir Path dataDir;

    private Database db;
    private BackupCatalogDao catalog;
    private BackupFileDao files;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        // FKs are required for the cascade-delete test.
        db.connection().createStatement().execute("PRAGMA foreign_keys = ON");
        catalog = new BackupCatalogDao(db);
        files = new BackupFileDao(db);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void insert_finalise_round_trip() {
        // policy_id is null so the FK to backup_policies doesn't bind in a
        // test that doesn't exercise policy DAO paths. Real runs attach
        // policy_id via the scheduler; here we want catalog coverage only.
        BackupCatalogRow running = new BackupCatalogRow(-1, null, "cx-1",
                1_000L, null, BackupStatus.RUNNING, 7L, "backups/nightly/2026-04-21",
                null, null, null, null, null, null, null, null);
        BackupCatalogRow saved = catalog.insert(running);
        assertTrue(saved.id() > 0);
        assertEquals(BackupStatus.RUNNING, catalog.byId(saved.id()).orElseThrow().status());

        catalog.finalise(saved.id(), BackupStatus.OK, 2_000L,
                "a".repeat(64), 123_456_789L, 1_000_000L,
                1_713_527_000L, 1_713_528_000L, "done");
        BackupCatalogRow done = catalog.byId(saved.id()).orElseThrow();
        assertEquals(BackupStatus.OK, done.status());
        assertEquals(2_000L, done.finishedAt());
        assertEquals(123_456_789L, done.totalBytes());
        assertEquals(1_713_528_000L, done.oplogLastTs());
        assertEquals("a".repeat(64), done.manifestSha256());
    }

    @Test
    void listForConnection_is_newest_first() {
        catalog.insert(sample(1_000L, "cx-a"));
        catalog.insert(sample(2_000L, "cx-a"));
        catalog.insert(sample(1_500L, "cx-b"));
        List<BackupCatalogRow> a = catalog.listForConnection("cx-a", 10);
        assertEquals(2, a.size());
        assertEquals(2_000L, a.get(0).startedAt());
    }

    @Test
    void recordVerification_sets_outcome() {
        BackupCatalogRow saved = catalog.insert(sample(1_000L, "cx-a"));
        catalog.recordVerification(saved.id(), 3_000L, "VERIFIED");
        BackupCatalogRow back = catalog.byId(saved.id()).orElseThrow();
        assertEquals(3_000L, back.verifiedAt());
        assertEquals("VERIFIED", back.verifyOutcome());
    }

    @Test
    void files_cascade_delete_when_catalog_row_is_removed() {
        BackupCatalogRow saved = catalog.insert(sample(1_000L, "cx-a"));
        files.insertAll(List.of(
                new BackupFileRow(-1, saved.id(), "dump/a.bson", 10, "b".repeat(64),
                        "db", "coll-a", "bson"),
                new BackupFileRow(-1, saved.id(), "dump/b.bson", 20, "c".repeat(64),
                        "db", "coll-b", "bson")));
        assertEquals(2, files.listForCatalog(saved.id()).size());

        // deleteOlderThan doesn't delete RUNNING rows (finished_at IS NULL);
        // flip to OK and re-invoke with a high cutoff.
        catalog.finalise(saved.id(), BackupStatus.OK, 2_000L,
                "h".repeat(64), 30L, 2L, null, null, null);
        catalog.deleteOlderThan(10_000L);
        assertTrue(catalog.byId(saved.id()).isEmpty());
        assertTrue(files.listForCatalog(saved.id()).isEmpty(),
                "child file rows cascade-deleted");
    }

    @Test
    void running_row_is_not_purged_by_retention() {
        BackupCatalogRow saved = catalog.insert(sample(1L, "cx-a"));
        catalog.deleteOlderThan(Long.MAX_VALUE);
        assertTrue(catalog.byId(saved.id()).isPresent(),
                "finished_at IS NULL must survive retention purge");
    }

    private static BackupCatalogRow sample(long startedAt, String cx) {
        return new BackupCatalogRow(-1, null, cx, startedAt, null, BackupStatus.RUNNING,
                7L, "p/" + startedAt, null, null, null, null, null, null, null, null);
    }
}
