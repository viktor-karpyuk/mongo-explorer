package com.kubrik.mex.migration.verify;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;

/** Self-contained HTML rendering of a {@link VerificationReport}. Inline CSS, no external
 *  fetches — the file opens cleanly in any browser with no network (SAFE-6). */
public final class HtmlReportRenderer {

    private HtmlReportRenderer() {}

    public static String render(VerificationReport report) {
        StringBuilder sb = new StringBuilder(4096);
        sb.append("""
                <!doctype html>
                <html lang="en">
                <head>
                  <meta charset="utf-8">
                  <title>Migration verification report</title>
                  <style>
                    :root { --pass: #16a34a; --warn: #b45309; --fail: #dc2626; --muted: #6b7280; }
                    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                           margin: 0; padding: 24px; color: #111827; }
                    h1 { margin: 0 0 4px 0; font-size: 22px; }
                    .muted { color: var(--muted); font-size: 13px; }
                    .pill { display: inline-block; padding: 2px 10px; border-radius: 9999px;
                            font-weight: 600; font-size: 12px; letter-spacing: 0.05em; margin-left: 8px; }
                    .pill.pass { background: #dcfce7; color: var(--pass); }
                    .pill.warn { background: #fef3c7; color: var(--warn); }
                    .pill.fail { background: #fee2e2; color: var(--fail); }
                    table { border-collapse: collapse; margin-top: 20px; width: 100%; font-size: 13px; }
                    th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid #e5e7eb; }
                    th { background: #f9fafb; font-weight: 600; font-size: 12px;
                         letter-spacing: 0.02em; color: #374151; }
                    tr.fail td { background: #fef2f2; }
                    tr.warn td { background: #fffbeb; }
                    .indexDiff { color: var(--warn); font-size: 12px; }
                  </style>
                </head>
                <body>
                """);
        String statusClass = report.status().name().toLowerCase();
        sb.append("<h1>Migration verification")
          .append("<span class=\"pill ").append(statusClass).append("\">")
          .append(report.status().name()).append("</span></h1>\n");
        sb.append("<div class=\"muted\">Job <code>").append(escape(report.jobId())).append("</code>")
          .append(" &middot; generated ").append(DateTimeFormatter.ISO_INSTANT.format(report.generatedAt()))
          .append(" &middot; engine ").append(escape(report.engineVersion()))
          .append("</div>\n");

        sb.append("""
                <table>
                  <thead>
                    <tr>
                      <th>Source</th><th>Target</th>
                      <th>Source count</th><th>Target count</th>
                      <th>Sample</th><th>Transformed</th>
                      <th>Full hash</th><th>Index diff</th>
                    </tr>
                  </thead>
                  <tbody>
                """);
        for (VerificationReport.CollectionReport c : report.collections()) {
            boolean hashMismatch = c.fullHash() != null && c.fullHash().startsWith("MISMATCH");
            String rowClass = !c.countMatch() || c.sampleMismatches() > 0 || hashMismatch ? "fail"
                    : !c.indexDiff().isEmpty() ? "warn" : "";
            sb.append("<tr").append(rowClass.isEmpty() ? "" : " class=\"" + rowClass + "\"").append(">\n")
              .append("<td>").append(escape(c.source())).append("</td>")
              .append("<td>").append(escape(c.target())).append("</td>")
              .append("<td>").append(c.countSource()).append("</td>")
              .append("<td>").append(c.countTarget()).append("</td>")
              .append("<td>").append(c.sampleSize() == 0 ? "—" :
                      c.sampleMismatches() + "/" + c.sampleSize() + " mismatches").append("</td>")
              .append("<td>").append(c.transformed() ? "yes" : "no").append("</td>")
              .append("<td>").append(c.fullHash() == null ? "—" : escape(c.fullHash())).append("</td>")
              .append("<td class=\"indexDiff\">")
              .append(c.indexDiff().isEmpty() ? "" : escape(String.join("; ", c.indexDiff())))
              .append("</td></tr>\n");
        }
        sb.append("</tbody></table>\n</body></html>\n");
        return sb.toString();
    }

    public static Path write(VerificationReport report, Path jobDir) throws IOException {
        Path out = jobDir.resolve("verification.html");
        Files.createDirectories(jobDir);
        Files.writeString(out, render(report));
        return out;
    }

    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;");
    }
}
