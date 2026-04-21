package com.kubrik.mex.backup.verify;

import com.kubrik.mex.backup.manifest.FileHasher;
import com.kubrik.mex.backup.store.BackupCatalogDao;
import com.kubrik.mex.backup.store.BackupCatalogRow;
import com.kubrik.mex.backup.store.BackupFileDao;
import com.kubrik.mex.backup.store.BackupFileRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

/**
 * v2.5 Q2.5-D — integrity verifier for a persisted backup.
 *
 * <p>Checks, in order:</p>
 * <ol>
 *   <li>The manifest file exists on disk at
 *       {@code <sinkRoot>/<sinkPath>/manifest.json}.</li>
 *   <li>Its bytes hash to {@link BackupCatalogRow#manifestSha256} — proves
 *       the manifest hasn't been tampered with after the run.</li>
 *   <li>Every {@code backup_files} row for the catalog id resolves to an
 *       existing file whose SHA-256 matches the stored hash.</li>
 * </ol>
 *
 * <p>Writes the outcome through
 * {@link BackupCatalogDao#recordVerification}; the {@link VerifyReport}
 * returned by {@link #verify} carries per-problem detail for the UI.</p>
 */
public final class CatalogVerifier {

    private static final Logger log = LoggerFactory.getLogger(CatalogVerifier.class);

    private final BackupCatalogDao catalog;
    private final BackupFileDao files;
    private final Path sinkRoot;
    private final Clock clock;

    public CatalogVerifier(BackupCatalogDao catalog, BackupFileDao files,
                           Path sinkRoot, Clock clock) {
        this.catalog = catalog;
        this.files = files;
        this.sinkRoot = sinkRoot;
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    public VerifyReport verify(long catalogId) {
        BackupCatalogRow row = catalog.byId(catalogId).orElse(null);
        if (row == null) {
            return new VerifyReport(catalogId, VerifyOutcome.MANIFEST_MISSING,
                    0, 0, List.of("catalog row " + catalogId + " not found"));
        }
        List<String> problems = new ArrayList<>();
        long checked = 0;
        long bytes = 0;
        Path backupDir = sinkRoot.resolve(row.sinkPath());
        Path manifestPath = backupDir.resolve("manifest.json");

        if (!Files.exists(manifestPath)) {
            return finalise(catalogId, VerifyOutcome.MANIFEST_MISSING, 0, 0,
                    List.of("manifest.json not found at " + manifestPath));
        }

        if (row.manifestSha256() != null) {
            try {
                String bytesSha = FileHasher.hashBytes(
                        Files.readAllBytes(manifestPath));
                // Footer sha is computed from the canonical JSON *excluding* the
                // hash itself, which is what the manifest file contents already
                // are. Mismatch means the file was edited after the run.
                if (!bytesSha.equals(row.manifestSha256())) {
                    problems.add("manifest footer hash mismatch (expected "
                            + row.manifestSha256() + ", got " + bytesSha + ")");
                    return finalise(catalogId, VerifyOutcome.MANIFEST_TAMPERED,
                            checked, bytes, problems);
                }
            } catch (IOException e) {
                return finalise(catalogId, VerifyOutcome.IO_ERROR, checked, bytes,
                        List.of("manifest read failed: " + e.getMessage()));
            }
        }

        for (BackupFileRow fr : files.listForCatalog(catalogId)) {
            Path abs = sinkRoot.resolve(fr.relativePath());
            if (!Files.exists(abs)) {
                problems.add("missing: " + fr.relativePath());
                continue;
            }
            try {
                long actualBytes = Files.size(abs);
                String actualSha = FileHasher.hashFile(abs);
                if (actualBytes != fr.bytes()) {
                    problems.add("size mismatch: " + fr.relativePath()
                            + " (expected " + fr.bytes() + ", got " + actualBytes + ")");
                }
                if (!actualSha.equals(fr.sha256())) {
                    problems.add("sha256 mismatch: " + fr.relativePath()
                            + " (expected " + fr.sha256() + ", got " + actualSha + ")");
                }
                checked++;
                bytes += actualBytes;
            } catch (IOException e) {
                problems.add("io error on " + fr.relativePath() + ": " + e.getMessage());
            }
        }

        VerifyOutcome outcome = problems.isEmpty() ? VerifyOutcome.VERIFIED
                : problems.stream().anyMatch(p -> p.startsWith("missing"))
                        ? VerifyOutcome.FILE_MISSING
                        : problems.stream().anyMatch(p -> p.contains("io error"))
                                ? VerifyOutcome.IO_ERROR
                                : VerifyOutcome.FILE_MISMATCH;
        return finalise(catalogId, outcome, checked, bytes, problems);
    }

    private VerifyReport finalise(long catalogId, VerifyOutcome outcome,
                                  long checked, long bytes, List<String> problems) {
        try {
            catalog.recordVerification(catalogId, clock.millis(), outcome.name());
        } catch (Exception e) {
            log.warn("recordVerification failed for catalog {}: {}", catalogId, e.getMessage());
        }
        return new VerifyReport(catalogId, outcome, checked, bytes, problems);
    }

    @SuppressWarnings("unused")
    private static String toHex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) sb.append(String.format("%02x", x));
        return sb.toString();
    }

    // Suppress the unused Charset import warning on toolchains that complain
    // about @SuppressWarnings-but-no-warning; placeholder for future use.
    @SuppressWarnings("unused")
    private static final java.nio.charset.Charset CHARSET = StandardCharsets.UTF_8;
}
