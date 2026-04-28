package com.kubrik.mex.maint.upgrade;

import com.kubrik.mex.maint.model.UpgradePlan;
import com.kubrik.mex.maint.model.UpgradePlan.Version;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RunbookRendererTest {

    private final UpgradeScanner scanner = new UpgradeScanner();
    private final RunbookRenderer renderer = new RunbookRenderer();

    @Test
    void markdown_contains_source_target_and_all_steps() {
        UpgradePlan.Plan plan = scanner.scan(
                new UpgradeRules.Context(Version.parse("6.0"),
                        Version.parse("7.0"), List.of(), List.of()),
                List.of("h1", "h2", "h3"));
        String md = renderer.renderMarkdown(plan);

        assertTrue(md.contains("# MongoDB Upgrade Runbook"));
        assertTrue(md.contains("**Source:** 6.0.0"));
        assertTrue(md.contains("**Target:** 7.0.0"));
        // Every step's title appears.
        for (UpgradePlan.Step s : plan.steps()) {
            assertTrue(md.contains(s.title()),
                    "runbook missing step '" + s.title() + "'");
        }
    }

    @Test
    void html_escapes_angle_brackets() {
        // Craft a plan with an angle-bracketed title so the escaper
        // has something real to work with.
        UpgradePlan.Plan plan = new UpgradePlan.Plan(
                Version.parse("6.0"), Version.parse("7.0"),
                List.of(new UpgradePlan.Finding(
                        UpgradePlan.FindingKind.OP_DEPRECATED,
                        "TEST", UpgradePlan.Severity.WARN,
                        "Avoid <script> tag in titles",
                        "detail", null)),
                List.of());
        String html = renderer.renderHtml(plan);
        assertTrue(html.contains("&lt;script&gt;"),
                "angle brackets in user content must be escaped");
        assertFalse(html.contains("<script>"));
    }

    @Test
    void html_document_is_well_formed_head_and_body() {
        UpgradePlan.Plan plan = scanner.scan(
                new UpgradeRules.Context(Version.parse("5.0"),
                        Version.parse("6.0"), List.of(), List.of()),
                List.of("h1"));
        String html = renderer.renderHtml(plan);
        assertTrue(html.startsWith("<!doctype html>"));
        assertTrue(html.contains("<head>"));
        assertTrue(html.contains("</body></html>"));
    }
}
