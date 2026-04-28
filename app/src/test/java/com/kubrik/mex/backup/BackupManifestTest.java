package com.kubrik.mex.backup;

import com.kubrik.mex.backup.manifest.BackupManifest;
import com.kubrik.mex.backup.manifest.FileHasher;
import com.kubrik.mex.backup.manifest.FileRecord;
import com.kubrik.mex.backup.manifest.OplogSlice;
import com.kubrik.mex.backup.spec.ArchiveSpec;
import com.kubrik.mex.backup.spec.Scope;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.5 BKP-RUN-5 — manifest canonical JSON + footer hash stability.
 * Shuffling the files array or swapping scope variants must change the
 * hash; identical content must produce identical hash bytes.
 */
class BackupManifestTest {

    @TempDir Path tmp;

    private static final String ZERO_SHA = "0".repeat(64);

    @Test
    void footer_hash_is_stable_across_identical_inputs() {
        BackupManifest a = sample();
        BackupManifest b = sample();
        assertEquals(a.footerSha256(), b.footerSha256());
    }

    @Test
    void footer_hash_sorts_files_deterministically() {
        FileRecord fa = new FileRecord("a.bson", 10, "a".repeat(64));
        FileRecord fb = new FileRecord("b.bson", 20, "b".repeat(64));
        BackupManifest ab = sampleWith(List.of(fa, fb));
        BackupManifest ba = sampleWith(List.of(fb, fa));
        assertEquals(ab.footerSha256(), ba.footerSha256(),
                "files array is sorted by path before hashing");
    }

    @Test
    void swapping_scope_changes_the_hash() {
        BackupManifest whole = sample();
        BackupManifest dbs = new BackupManifest(whole.mexVersion(), whole.manifestVersion(),
                whole.createdAt(), whole.policyId(), whole.connectionId(),
                new Scope.Databases(List.of("reports")), whole.archive(),
                whole.files(), whole.oplog());
        assertNotEquals(whole.footerSha256(), dbs.footerSha256());
    }

    @Test
    void totalBytes_sums_the_files_array() {
        BackupManifest m = sampleWith(List.of(
                new FileRecord("a", 100, ZERO_SHA),
                new FileRecord("b", 200, ZERO_SHA)));
        assertEquals(300, m.totalBytes());
    }

    @Test
    void canonical_json_is_parseable() {
        // The canonical output must still be valid JSON — Jackson parses it.
        String json = sample().toCanonicalJson();
        assertDoesNotThrow(() ->
                new com.fasterxml.jackson.databind.ObjectMapper().readTree(json));
        // Keys sorted: scope before version lexicographically means "archive"
        // appears before "connectionId" which appears before "files".
        int archPos = json.indexOf("\"archive\"");
        int connPos = json.indexOf("\"connectionId\"");
        int filesPos = json.indexOf("\"files\"");
        assertTrue(archPos < connPos && connPos < filesPos,
                "keys must be in sorted order");
    }

    @Test
    void fileHasher_produces_sha256_hex() throws Exception {
        Path f = tmp.resolve("blob.txt");
        Files.writeString(f, "hello v2.5", StandardCharsets.UTF_8);
        String hash = FileHasher.hashFile(f);
        assertEquals(64, hash.length());
        assertTrue(hash.matches("[0-9a-f]{64}"));
        // Same input → same digest.
        assertEquals(hash, FileHasher.hashBytes("hello v2.5".getBytes(StandardCharsets.UTF_8)));
    }

    /* ============================ fixtures ============================ */

    private static BackupManifest sample() {
        return sampleWith(List.of(new FileRecord("dump/coll.bson", 12_345, "f".repeat(64))));
    }

    private static BackupManifest sampleWith(List<FileRecord> files) {
        return new BackupManifest(
                "2.5.0", BackupManifest.MANIFEST_VERSION,
                Instant.ofEpochSecond(1_713_600_000L),
                42L, "prod-east",
                new Scope.WholeCluster(),
                ArchiveSpec.defaults(),
                files,
                new OplogSlice(1_713_527_000L, 1_713_528_000L, "oplog.bson.gz", "o".repeat(64)));
    }
}
