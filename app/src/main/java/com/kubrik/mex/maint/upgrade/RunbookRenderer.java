package com.kubrik.mex.maint.upgrade;

import com.kubrik.mex.maint.model.UpgradePlan;

/**
 * v2.7 UPG-4 — Renders an {@link UpgradePlan.Plan} into a
 * Markdown document the DBA can follow offline, plus an HTML pipe
 * for rendering in the Maintenance tab.
 *
 * <p>Dependency-free on purpose. The spec ({@code technical-spec §18}
 * hints at Mustache-java, but a curated plan never exceeds a few kB
 * and a tiny templateless renderer keeps the jpackage image lean.</p>
 */
public final class RunbookRenderer {

    public String renderMarkdown(UpgradePlan.Plan plan) {
        StringBuilder md = new StringBuilder();
        md.append("# MongoDB Upgrade Runbook\n\n");
        md.append("**Source:** ").append(plan.from().asFull()).append("  \n");
        md.append("**Target:** ").append(plan.to().asFull()).append("\n\n");

        if (!plan.findings().isEmpty()) {
            md.append("## Findings\n\n");
            for (UpgradePlan.Finding f : plan.findings()) {
                md.append("### ").append(badge(f.severity())).append(" ")
                        .append(f.title()).append(" (").append(f.code())
                        .append(")\n\n");
                md.append(f.detail()).append("\n\n");
                if (f.remediation() != null && !f.remediation().isBlank()) {
                    md.append("*Remediation:* ").append(f.remediation())
                            .append("\n\n");
                }
            }
        }

        md.append("## Steps\n\n");
        for (UpgradePlan.Step s : plan.steps()) {
            md.append(s.order()).append(". **").append(s.title()).append("**")
                    .append("  _(").append(s.kind().name().toLowerCase())
                    .append(")_\n");
            if (s.targetHost() != null) {
                md.append("    - host: `").append(s.targetHost()).append("`\n");
            }
            String[] bodyLines = s.body().split("\n");
            for (String line : bodyLines) {
                md.append("    ").append(line).append("\n");
            }
            md.append('\n');
        }
        return md.toString();
    }

    public String renderHtml(UpgradePlan.Plan plan) {
        StringBuilder h = new StringBuilder();
        h.append("<!doctype html>\n<html><head><meta charset='utf-8'>\n");
        h.append("<title>MongoDB Upgrade Runbook</title>\n");
        h.append("<style>body{font-family:system-ui,sans-serif;max-width:860px;")
                .append("margin:2em auto;padding:0 1em;line-height:1.55;color:#111827}")
                .append("h1,h2,h3{color:#0f172a}code{background:#f1f5f9;padding:1px 4px;")
                .append("border-radius:3px}.badge{display:inline-block;padding:1px 8px;")
                .append("border-radius:10px;font-size:11px;font-weight:600;color:#fff}")
                .append(".b-INFO{background:#64748b}.b-WARN{background:#d97706}")
                .append(".b-BLOCK{background:#b91c1c}ol li{margin-bottom:12px}")
                .append("pre{background:#f8fafc;padding:8px;border-radius:6px;overflow:auto}</style>\n");
        h.append("</head><body>\n");
        h.append("<h1>MongoDB Upgrade Runbook</h1>\n");
        h.append("<p><b>Source:</b> ").append(escape(plan.from().asFull())).append("<br>\n");
        h.append("<b>Target:</b> ").append(escape(plan.to().asFull())).append("</p>\n");

        if (!plan.findings().isEmpty()) {
            h.append("<h2>Findings</h2>\n");
            for (UpgradePlan.Finding f : plan.findings()) {
                h.append("<h3><span class='badge b-").append(f.severity().name())
                        .append("'>").append(f.severity().name()).append("</span> ")
                        .append(escape(f.title())).append(" <code>")
                        .append(escape(f.code())).append("</code></h3>\n");
                h.append("<p>").append(escape(f.detail())).append("</p>\n");
                if (f.remediation() != null && !f.remediation().isBlank()) {
                    h.append("<p><em>Remediation:</em> ")
                            .append(escape(f.remediation())).append("</p>\n");
                }
            }
        }

        h.append("<h2>Steps</h2>\n<ol>\n");
        for (UpgradePlan.Step s : plan.steps()) {
            h.append("<li><b>").append(escape(s.title())).append("</b>")
                    .append(" <em>(").append(s.kind().name().toLowerCase())
                    .append(")</em>");
            if (s.targetHost() != null) {
                h.append("<br>host: <code>")
                        .append(escape(s.targetHost())).append("</code>");
            }
            h.append("<pre>").append(escape(s.body())).append("</pre></li>\n");
        }
        h.append("</ol>\n</body></html>\n");
        return h.toString();
    }

    private static String badge(UpgradePlan.Severity s) {
        return switch (s) {
            case INFO -> "\u2139\uFE0F";
            case WARN -> "\u26A0\uFE0F";
            case BLOCK -> "\uD83D\uDEAB";
        };
    }

    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;");
    }
}
