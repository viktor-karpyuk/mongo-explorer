package com.kubrik.mex.cluster.audit;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * v2.4 AUD-EXP-1..3 — exports {@link OpsAuditRecord} slices to disk.
 *
 * <ul>
 *   <li>{@link #writeJson} — writes a self-describing JSON bundle with a
 *       top-level envelope (generator, timestamp, row count) and an array
 *       of row objects; each row carries a {@code sha256} covering the
 *       canonical row shape so bundles can be tamper-checked.</li>
 *   <li>{@link #writeCsv} — writes a CSV with a fixed header row matching
 *       the spec's column list. Fields are CSV-escaped (quote-wrap +
 *       double-quote quoting) so pasted command JSON + server messages
 *       survive round-tripping through Excel / numbers.app.</li>
 * </ul>
 *
 * <p>Both writers stream a row at a time so selections larger than the JVM
 * heap are safe.</p>
 */
public final class AuditExporter {

    private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_INSTANT;

    private AuditExporter() {}

    public static void writeJson(Path target, List<OpsAuditRecord> rows) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(target, StandardCharsets.UTF_8)) {
            w.write("{\"generator\":\"mongo-explorer v2.4 audit exporter\",");
            w.write("\"exportedAt\":");
            w.write(jsonString(ISO.format(Instant.now())));
            w.write(",\"rowCount\":");
            w.write(String.valueOf(rows.size()));
            w.write(",\"rows\":[");
            boolean first = true;
            for (OpsAuditRecord r : rows) {
                if (!first) w.write(",");
                first = false;
                w.newLine();
                w.write(rowAsJson(r));
            }
            w.newLine();
            w.write("]}");
        }
    }

    public static void writeCsv(Path target, List<OpsAuditRecord> rows) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(target, StandardCharsets.UTF_8)) {
            w.write("id,connection_id,started_at,finished_at,latency_ms,command,"
                    + "outcome,role_used,ui_source,paste,kill_switch,"
                    + "server_message,preview_hash\n");
            for (OpsAuditRecord r : rows) {
                w.write(String.valueOf(r.id())); w.write(',');
                w.write(csvEscape(r.connectionId())); w.write(',');
                w.write(String.valueOf(r.startedAt())); w.write(',');
                w.write(r.finishedAt() == null ? "" : r.finishedAt().toString()); w.write(',');
                w.write(r.latencyMs() == null ? "" : r.latencyMs().toString()); w.write(',');
                w.write(csvEscape(r.commandName())); w.write(',');
                w.write(r.outcome().name()); w.write(',');
                w.write(csvEscape(r.roleUsed())); w.write(',');
                w.write(csvEscape(r.uiSource())); w.write(',');
                w.write(r.paste() ? "1" : "0"); w.write(',');
                w.write(r.killSwitch() ? "1" : "0"); w.write(',');
                w.write(csvEscape(r.serverMessage())); w.write(',');
                w.write(csvEscape(r.previewHash()));
                w.write('\n');
            }
        }
    }

    /* ========================== JSON helpers =========================== */

    static String rowAsJson(OpsAuditRecord r) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        kv(sb, "id", String.valueOf(r.id())); sb.append(',');
        kvStr(sb, "connection_id", r.connectionId()); sb.append(',');
        kv(sb, "started_at", String.valueOf(r.startedAt())); sb.append(',');
        kv(sb, "finished_at", r.finishedAt() == null ? "null" : r.finishedAt().toString()); sb.append(',');
        kv(sb, "latency_ms", r.latencyMs() == null ? "null" : r.latencyMs().toString()); sb.append(',');
        kvStr(sb, "command_name", r.commandName()); sb.append(',');
        kvStr(sb, "outcome", r.outcome().name()); sb.append(',');
        kvStrNullable(sb, "server_message", r.serverMessage()); sb.append(',');
        kvStrNullable(sb, "role_used", r.roleUsed()); sb.append(',');
        kvStr(sb, "ui_source", r.uiSource()); sb.append(',');
        kv(sb, "paste", r.paste() ? "true" : "false"); sb.append(',');
        kv(sb, "kill_switch", r.killSwitch() ? "true" : "false"); sb.append(',');
        kvStr(sb, "preview_hash", r.previewHash()); sb.append(',');
        kvStr(sb, "command_json_redacted", r.commandJsonRedacted()); sb.append(',');
        kvStr(sb, "sha256", sha256RowDigest(r));
        sb.append('}');
        return sb.toString();
    }

    /** Stable SHA-256 over the fields that uniquely identify the row. Used
     *  by {@code AuditExportTest} and bundle-verification tools. */
    public static String sha256RowDigest(OpsAuditRecord r) {
        String blob = r.id() + "|" + r.connectionId() + "|" + r.startedAt()
                + "|" + r.commandName() + "|" + r.outcome()
                + "|" + r.previewHash()
                + "|" + r.commandJsonRedacted();
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(blob.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static void kv(StringBuilder sb, String key, String rawValue) {
        sb.append(jsonString(key)).append(':').append(rawValue);
    }

    private static void kvStr(StringBuilder sb, String key, String value) {
        sb.append(jsonString(key)).append(':').append(jsonString(value));
    }

    private static void kvStrNullable(StringBuilder sb, String key, String value) {
        if (value == null) sb.append(jsonString(key)).append(":null");
        else kvStr(sb, key, value);
    }

    static String jsonString(String s) {
        if (s == null) return "null";
        StringBuilder sb = new StringBuilder(s.length() + 2);
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"'  -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default   -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        sb.append('"');
        return sb.toString();
    }

    /* =========================== CSV helpers =========================== */

    static String csvEscape(String s) {
        if (s == null) return "";
        boolean needsQuote = s.indexOf(',') >= 0 || s.indexOf('"') >= 0
                || s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0;
        if (!needsQuote) return s;
        return '"' + s.replace("\"", "\"\"") + '"';
    }
}
