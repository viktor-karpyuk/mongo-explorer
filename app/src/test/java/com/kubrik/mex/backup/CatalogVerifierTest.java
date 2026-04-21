package com.kubrik.mex.backup;

import com.kubrik.mex.backup.manifest.FileHasher;
import com.kubrik.mex.backup.store.BackupCatalogDao;
import com.kubrik.mex.backup.store.BackupCatalogRow;
import com.kubrik.mex.backup.store.BackupFileDao;
import com.kubrik.mex.backup.store.BackupFileRow;
import com.kubrik.mex.backup.store.BackupStatus;
import com.kubrik.mex.backup.verify.CatalogVerifier;
import com.kubrik.mex.backup.verify.VerifyOutcome;
import com.kubrik.mex.backup.verify.VerifyReport;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.5 Q2.5-D — covers every VerifyOutcome branch the CatalogVerifier can
 * produce: manifest missing, manifest tampered, file missing, file
 * mismatch, and the happy path.
 */
class CatalogVerifierTest {

    @TempDir Path dataDir;
    @TempDir Path sinkRoot;

    private Database db;
    private BackupCatalogDao catalog;
    private BackupFileDao files;
    private CatalogVerifier verifier;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        catalog = new BackupCatalogDao(db);
        files = new BackupFileDao(db);
        verifier = new CatalogVerifier(catalog, files, sinkRoot, Clock.systemUTC());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void happy_path_reports_VERIFIED_and_records_on_catalog() throws IOException {
        long id = seedGoodBackup("good");
        VerifyReport report = verifier.verify(id);
        assertEquals(VerifyOutcome.VERIFIED, report.outcome());
        assertTrue(report.problems().isEmpty());
        assertTrue(report.filesChecked() >= 1);
        assertNotNull(catalog.byId(id).orElseThrow().verifyOutcome());
        assertEquals("VERIFIED", catalog.byId(id).orElseThrow().verifyOutcome());
    }

    @Test
    void missing_manifest_reports_MANIFEST_MISSING() {
        long id = seedCatalog("nomanifest", "h".repeat(64));
        VerifyReport report = verifier.verify(id);
        assertEquals(VerifyOutcome.MANIFEST_MISSING, report.outcome());
    }

    @Test
    void tampered_manifest_reports_MANIFEST_TAMPERED() throws IOException {
        long id = seedGoodBackup("tampered");
        Path manifestPath = sinkRoot.resolve("tampered/manifest.json");
        Files.writeString(manifestPath, "{\"forged\":true}", StandardCharsets.UTF_8);
        VerifyReport report = verifier.verify(id);
        assertEquals(VerifyOutcome.MANIFEST_TAMPERED, report.outcome());
        assertTrue(report.problems().get(0).contains("footer hash mismatch"));
    }

    @Test
    void missing_file_reports_FILE_MISSING() throws IOException {
        long id = seedGoodBackup("filegone");
        Files.delete(sinkRoot.resolve("filegone/dump/a.bson"));
        VerifyReport report = verifier.verify(id);
        assertEquals(VerifyOutcome.FILE_MISSING, report.outcome());
        assertTrue(report.problems().stream().anyMatch(p -> p.startsWith("missing")));
    }

    @Test
    void corrupted_file_reports_FILE_MISMATCH() throws IOException {
        long id = seedGoodBackup("flipped");
        Path f = sinkRoot.resolve("flipped/dump/a.bson");
        Files.writeString(f, "CORRUPTED", StandardCharsets.UTF_8);
        VerifyReport report = verifier.verify(id);
        assertEquals(VerifyOutcome.FILE_MISMATCH, report.outcome());
        assertTrue(report.problems().stream()
                .anyMatch(p -> p.contains("sha256 mismatch") || p.contains("size mismatch")));
    }

    /* ============================ fixtures ============================ */

    /** Seeds a catalog row + two files + a matching manifest on disk with
     *  hashes that line up, so verify() hits the happy path. */
    private long seedGoodBackup(String dir) throws IOException {
        Path backupDir = sinkRoot.resolve(dir);
        Files.createDirectories(backupDir.resolve("dump"));
        Path a = backupDir.resolve("dump/a.bson");
        Path b = backupDir.resolve("dump/b.bson");
        Files.writeString(a, "payload-a", StandardCharsets.UTF_8);
        Files.writeString(b, "payload-b", StandardCharsets.UTF_8);

        String manifest = "{\"ok\":true}";
        Path manifestPath = backupDir.resolve("manifest.json");
        Files.writeString(manifestPath, manifest, StandardCharsets.UTF_8);
        String manifestSha = FileHasher.hashBytes(manifest.getBytes(StandardCharsets.UTF_8));

        long catalogId = seedCatalog(dir, manifestSha);
        files.insertAll(List.of(
                new BackupFileRow(-1, catalogId, dir + "/dump/a.bson",
                        Files.size(a), FileHasher.hashFile(a), "db", "a", "bson"),
                new BackupFileRow(-1, catalogId, dir + "/dump/b.bson",
                        Files.size(b), FileHasher.hashFile(b), "db", "b", "bson")));
        return catalogId;
    }

    private long seedCatalog(String relPath, String manifestSha) {
        BackupCatalogRow row = new BackupCatalogRow(-1, null, "cx-it",
                1_000L, 2_000L, BackupStatus.OK, 1L, relPath,
                manifestSha, null, null, null, null, null, null, null);
        return catalog.insert(row).id();
    }
}
