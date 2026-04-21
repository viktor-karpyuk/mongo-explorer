package com.kubrik.mex.security.export;

import com.kubrik.mex.security.EvidenceSigner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

/**
 * v2.6 Q2.6-I — writes an evidence bundle: {@code report.json},
 * {@code report.html}, and {@code evidence.sig} (hex HMAC-SHA-256 over
 * the UTF-8 bytes of {@code report.json}, signed by {@link EvidenceSigner}).
 *
 * <p>The signature is <em>tamper-evidence</em>, not <em>authorship</em>:
 * anyone with the install's signing key can produce a matching bundle.
 * That's intentional — the DBA proves to an auditor that the JSON they
 * filed away matches what the tool produced, not that the tool itself
 * is trustworthy. The verifier UI copy (built with Q2.6-I2) spells this
 * out alongside the verdict.</p>
 *
 * <p>This class is intentionally small: one static {@link #export} and
 * one static {@link #verify}. Report-specific HTML rendering lives with
 * each consumer (CIS scan, drift diff, cert inventory) so the export
 * surface doesn't need to know the semantic shape.</p>
 */
public final class EvidenceBundle {

    public static final String REPORT_JSON = "report.json";
    public static final String REPORT_HTML = "report.html";
    public static final String EVIDENCE_SIG = "evidence.sig";

    private EvidenceBundle() {}

    public record Exported(Path dir, String signature, long writtenAtMs) {}

    /**
     * Write the three files to {@code targetDir}. Creates the directory
     * if missing; overwrites existing files. Returns the {@link Exported}
     * handle so the caller can show the signature in the success toast.
     */
    public static Exported export(Path targetDir, String reportJson,
                                    String reportHtml, EvidenceSigner signer) throws IOException {
        if (reportJson == null) throw new IllegalArgumentException("reportJson");
        if (signer == null) throw new IllegalArgumentException("signer");
        Files.createDirectories(targetDir);

        Path jsonPath = targetDir.resolve(REPORT_JSON);
        Files.writeString(jsonPath, reportJson, StandardCharsets.UTF_8);

        if (reportHtml != null) {
            Files.writeString(targetDir.resolve(REPORT_HTML), reportHtml,
                    StandardCharsets.UTF_8);
        }

        // Signature is over the JSON bytes that landed on disk — not any
        // in-memory representation — so a later verify() pass reads the
        // same bytes we signed.
        byte[] jsonBytes = Files.readAllBytes(jsonPath);
        String sig = signer.sign(jsonBytes);
        Files.writeString(targetDir.resolve(EVIDENCE_SIG), sig,
                StandardCharsets.UTF_8);

        return new Exported(targetDir, sig, Instant.now().toEpochMilli());
    }

    /**
     * Verify a previously-exported bundle. Reads {@code report.json} +
     * {@code evidence.sig} from {@code dir} and recomputes the HMAC. Any
     * missing file or signature mismatch returns
     * {@link Verdict#TAMPERED}. A {@link Verdict#OK} result is the only
     * case that the UI should treat as proof-of-non-tampering.
     */
    public static Verdict verify(Path dir, EvidenceSigner signer) {
        try {
            Path jsonPath = dir.resolve(REPORT_JSON);
            Path sigPath = dir.resolve(EVIDENCE_SIG);
            if (!Files.exists(jsonPath) || !Files.exists(sigPath)) {
                return Verdict.MISSING;
            }
            byte[] jsonBytes = Files.readAllBytes(jsonPath);
            String expected = Files.readString(sigPath, StandardCharsets.UTF_8).trim();
            return signer.verify(jsonBytes, expected) ? Verdict.OK : Verdict.TAMPERED;
        } catch (IOException e) {
            return Verdict.IO_ERROR;
        }
    }

    public enum Verdict { OK, TAMPERED, MISSING, IO_ERROR }
}
