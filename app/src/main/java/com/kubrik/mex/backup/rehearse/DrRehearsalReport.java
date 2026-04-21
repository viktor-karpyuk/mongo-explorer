package com.kubrik.mex.backup.rehearse;

import com.kubrik.mex.cluster.audit.OpsAuditRecord;
import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.store.OpsAuditDao;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * v2.5 Q2.5-G — DR rehearsal report builder.
 *
 * <p>Pulls every {@code ops_audit} row with
 * {@code command_name = "restore.rehearse"} inside a window, classifies
 * each row by {@link Outcome}, and writes a JSON bundle plus an HTML
 * summary. The HTML is a single static file (no CSS framework) — tables
 * with inline styles so it renders cleanly when emailed or pinned to a
 * runbook.</p>
 */
public final class DrRehearsalReport {

    public static final String COMMAND_NAME = "restore.rehearse";

    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'").withZone(ZoneId.of("UTC"));

    private final OpsAuditDao auditDao;

    public DrRehearsalReport(OpsAuditDao auditDao) { this.auditDao = auditDao; }

    public Bundle build(long sinceMs, int limit) {
        List<OpsAuditRecord> rows = auditDao.listSince(sinceMs, limit).stream()
                .filter(r -> COMMAND_NAME.equals(r.commandName()))
                .toList();
        long ok = rows.stream().filter(r -> r.outcome() == Outcome.OK).count();
        long fail = rows.stream().filter(r -> r.outcome() == Outcome.FAIL).count();
        long cancelled = rows.stream().filter(r -> r.outcome() == Outcome.CANCELLED).count();
        return new Bundle(Instant.ofEpochMilli(sinceMs), Instant.now(),
                rows.size(), ok, fail, cancelled, rows);
    }

    public void writeJson(Bundle bundle, Path target) throws IOException {
        try (Writer w = Files.newBufferedWriter(target, StandardCharsets.UTF_8)) {
            w.write("{\"generator\":\"mongo-explorer v2.5 DR rehearsal report\",");
            w.write("\"sinceAt\":\"" + bundle.sinceAt() + "\",");
            w.write("\"generatedAt\":\"" + bundle.generatedAt() + "\",");
            w.write("\"rowCount\":" + bundle.rowCount() + ",");
            w.write("\"ok\":" + bundle.ok() + ",");
            w.write("\"fail\":" + bundle.fail() + ",");
            w.write("\"cancelled\":" + bundle.cancelled() + ",");
            w.write("\"rows\":[");
            boolean first = true;
            for (OpsAuditRecord r : bundle.rows()) {
                if (!first) w.write(",");
                first = false;
                w.write("\n  " + rowAsJson(r));
            }
            w.write("\n]}");
        }
    }

    public void writeHtml(Bundle bundle, Path target) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("<!DOCTYPE html><html><head><meta charset=\"utf-8\">")
                .append("<title>DR rehearsal report</title></head><body style=\"")
                .append("font-family: -apple-system, BlinkMacSystemFont, 'Helvetica Neue', sans-serif;")
                .append("color: #1f2937; padding: 24px; max-width: 1080px; margin: 0 auto;\">");
        sb.append("<h1 style=\"margin-bottom:4px;\">DR rehearsal report</h1>");
        sb.append("<p style=\"color:#6b7280;font-size:12px;\">")
                .append("Window: ").append(bundle.sinceAt()).append(" → ")
                .append(bundle.generatedAt())
                .append("</p>");
        sb.append("<p>")
                .append("<span style=\"background:#dcfce7;color:#166534;padding:2px 10px;border-radius:999px;font-weight:700;\">")
                .append(bundle.ok()).append(" ok").append("</span> ")
                .append("<span style=\"background:#fee2e2;color:#991b1b;padding:2px 10px;border-radius:999px;font-weight:700;margin-left:6px;\">")
                .append(bundle.fail()).append(" fail").append("</span> ")
                .append("<span style=\"background:#f3f4f6;color:#374151;padding:2px 10px;border-radius:999px;font-weight:700;margin-left:6px;\">")
                .append(bundle.cancelled()).append(" cancelled").append("</span> ")
                .append("<span style=\"color:#6b7280;margin-left:10px;font-size:12px;\">")
                .append(bundle.rowCount()).append(" rehearsals in window")
                .append("</span></p>");
        sb.append("<table style=\"width:100%;border-collapse:collapse;font-size:12px;\">")
                .append("<thead><tr style=\"border-bottom:1px solid #e5e7eb;text-align:left;\">")
                .append(th("started")).append(th("connection")).append(th("outcome"))
                .append(th("latency")).append(th("message"))
                .append("</tr></thead><tbody>");
        for (OpsAuditRecord r : bundle.rows()) {
            sb.append("<tr style=\"border-bottom:1px solid #f3f4f6;\">")
                    .append(td(TS_FMT.format(Instant.ofEpochMilli(r.startedAt()))))
                    .append(td(escape(r.connectionId())))
                    .append(td(outcomePill(r.outcome())))
                    .append(td(r.latencyMs() == null ? "—" : r.latencyMs() + " ms"))
                    .append(td(escape(r.serverMessage() == null ? "" : r.serverMessage())))
                    .append("</tr>");
        }
        sb.append("</tbody></table>");
        sb.append("<p style=\"color:#6b7280;font-size:11px;margin-top:24px;\">")
                .append("Generated by mongo-explorer v2.5 DR rehearsal report.</p>");
        sb.append("</body></html>");
        Files.writeString(target, sb.toString(), StandardCharsets.UTF_8);
    }

    /* ============================ bundle + json ============================ */

    public record Bundle(Instant sinceAt, Instant generatedAt,
                         long rowCount, long ok, long fail, long cancelled,
                         List<OpsAuditRecord> rows) {}

    static String rowAsJson(OpsAuditRecord r) {
        return "{\"id\":" + r.id()
                + ",\"connectionId\":\"" + escapeJson(r.connectionId()) + "\""
                + ",\"startedAt\":" + r.startedAt()
                + ",\"finishedAt\":" + (r.finishedAt() == null ? "null" : r.finishedAt())
                + ",\"latencyMs\":" + (r.latencyMs() == null ? "null" : r.latencyMs())
                + ",\"outcome\":\"" + r.outcome().name() + "\""
                + ",\"message\":\"" + escapeJson(r.serverMessage() == null ? "" : r.serverMessage()) + "\""
                + "}";
    }

    /* ============================= tiny helpers ============================= */

    private static String th(String label) {
        return "<th style=\"padding:6px 8px;color:#6b7280;font-weight:600;\">"
                + escape(label) + "</th>";
    }

    private static String td(String content) {
        return "<td style=\"padding:6px 8px;vertical-align:top;\">" + content + "</td>";
    }

    private static String outcomePill(Outcome o) {
        String bg, fg;
        switch (o) {
            case OK -> { bg = "#dcfce7"; fg = "#166534"; }
            case FAIL -> { bg = "#fee2e2"; fg = "#991b1b"; }
            case CANCELLED -> { bg = "#fef3c7"; fg = "#92400e"; }
            default -> { bg = "#f3f4f6"; fg = "#374151"; }
        }
        return "<span style=\"background:" + bg + ";color:" + fg
                + ";padding:2px 8px;border-radius:999px;font-weight:700;\">"
                + o.name() + "</span>";
    }

    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;");
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        StringBuilder sb = new StringBuilder(s.length() + 2);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"'  -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default   -> sb.append(c < 0x20 ? String.format("\\u%04x", (int) c) : c);
            }
        }
        return sb.toString();
    }
}
