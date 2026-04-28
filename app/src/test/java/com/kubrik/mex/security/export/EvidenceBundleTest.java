package com.kubrik.mex.security.export;

import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.security.EvidenceSigner;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-I — evidence-bundle round-trip. Export + verify + tamper
 * detection. Uses the real EvidenceSigner backed by a temp Database so
 * the signing key is generated on first use exactly as it would at
 * runtime.
 */
class EvidenceBundleTest {

    @TempDir Path home;
    @TempDir Path targetDir;
    private Database db;
    private EvidenceSigner signer;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", home.toString());
        db = new Database();
        signer = new EvidenceSigner(db, new Crypto());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void export_writes_all_three_files_and_verify_passes() throws Exception {
        String json = "{\"report\":\"cis-v1.2\",\"total\":42}";
        String html = "<html><body>42 rules</body></html>";

        EvidenceBundle.Exported out = EvidenceBundle.export(targetDir, json, html, signer);

        assertTrue(Files.exists(out.dir().resolve(EvidenceBundle.REPORT_JSON)));
        assertTrue(Files.exists(out.dir().resolve(EvidenceBundle.REPORT_HTML)));
        assertTrue(Files.exists(out.dir().resolve(EvidenceBundle.EVIDENCE_SIG)));
        assertEquals(64, out.signature().length());

        assertEquals(EvidenceBundle.Verdict.OK, EvidenceBundle.verify(targetDir, signer));
    }

    @Test
    void tampering_the_json_flips_verify_to_TAMPERED() throws Exception {
        EvidenceBundle.export(targetDir, "{\"v\":1}", null, signer);
        Path jsonPath = targetDir.resolve(EvidenceBundle.REPORT_JSON);
        Files.writeString(jsonPath, "{\"v\":2}", StandardCharsets.UTF_8);

        assertEquals(EvidenceBundle.Verdict.TAMPERED,
                EvidenceBundle.verify(targetDir, signer));
    }

    @Test
    void missing_signature_file_returns_MISSING() throws Exception {
        EvidenceBundle.export(targetDir, "{\"v\":1}", null, signer);
        Files.delete(targetDir.resolve(EvidenceBundle.EVIDENCE_SIG));

        assertEquals(EvidenceBundle.Verdict.MISSING,
                EvidenceBundle.verify(targetDir, signer));
    }

    @Test
    void missing_report_json_returns_MISSING() throws Exception {
        EvidenceBundle.export(targetDir, "{\"v\":1}", null, signer);
        Files.delete(targetDir.resolve(EvidenceBundle.REPORT_JSON));

        assertEquals(EvidenceBundle.Verdict.MISSING,
                EvidenceBundle.verify(targetDir, signer));
    }

    @Test
    void html_is_optional() throws Exception {
        EvidenceBundle.Exported out = EvidenceBundle.export(targetDir,
                "{\"v\":1}", null, signer);
        assertFalse(Files.exists(out.dir().resolve(EvidenceBundle.REPORT_HTML)));
        assertEquals(EvidenceBundle.Verdict.OK,
                EvidenceBundle.verify(targetDir, signer));
    }

    @Test
    void signature_covers_the_bytes_on_disk_not_the_in_memory_string() throws Exception {
        // Edge case: if we signed the caller's String and the on-disk
        // encoding differed (BOM, alternate line endings), verify would
        // silently fail. We sign the file bytes after write to guarantee
        // the signed payload is the payload that can be re-read.
        String json = "line1\nline2\n";   // LF endings
        EvidenceBundle.Exported out = EvidenceBundle.export(targetDir, json, null, signer);

        byte[] onDisk = Files.readAllBytes(targetDir.resolve(EvidenceBundle.REPORT_JSON));
        assertEquals(signer.sign(onDisk), out.signature());
        assertEquals(EvidenceBundle.Verdict.OK,
                EvidenceBundle.verify(targetDir, signer));
    }
}
