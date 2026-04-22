package com.kubrik.mex.maint.upgrade;

import com.kubrik.mex.maint.model.UpgradePlan;
import com.kubrik.mex.maint.model.UpgradePlan.Version;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 Q2.7-J — Pump random ASCII-ish strings through every
 * {@link UpgradePlan.Finding} field; the renderer must never produce
 * unescaped HTML or drop the step list.
 */
class RunbookMarkdownFuzz {

    private final RunbookRenderer renderer = new RunbookRenderer();

    @Test
    void random_titles_and_details_do_not_break_html_structure() {
        Random r = new Random(0xDEAD_BEEF);
        for (int i = 0; i < 200; i++) {
            String title = randomAscii(r, 80);
            String detail = randomAscii(r, 200);
            UpgradePlan.Plan plan = new UpgradePlan.Plan(
                    Version.parse("6.0"), Version.parse("7.0"),
                    List.of(new UpgradePlan.Finding(
                            UpgradePlan.FindingKind.OP_DEPRECATED,
                            "FUZZ-" + i, UpgradePlan.Severity.WARN,
                            title, detail, null)),
                    List.of());
            String html = renderer.renderHtml(plan);
            // Structural invariants — the fuzz input must not close
            // the body early or break the outer shell.
            assertTrue(html.startsWith("<!doctype html>"));
            assertTrue(html.endsWith("</body></html>\n"));
            // Raw '<' in the fuzz MUST be escaped. A literal unescaped
            // '<script' would indicate an injection vulnerability.
            assertFalse(html.contains("<script"),
                    "fuzz #" + i + " produced unescaped <script");
        }
    }

    @Test
    void random_step_bodies_rendered_literally_not_as_HTML() {
        Random r = new Random(42);
        for (int i = 0; i < 100; i++) {
            String body = randomAscii(r, 500);
            UpgradePlan.Plan plan = new UpgradePlan.Plan(
                    Version.parse("6.0"), Version.parse("7.0"),
                    List.of(),
                    List.of(new UpgradePlan.Step(1,
                            UpgradePlan.StepKind.BINARY_SWAP,
                            "Fuzz step", body, "h" + i + ":27017")));
            String html = renderer.renderHtml(plan);
            // The body lands inside a <pre>; the pre's content should
            // be escaped so even strings that look like HTML arrive
            // literally. If the fuzz produced '<', it must appear as
            // '&lt;'.
            if (body.contains("<")) {
                assertTrue(html.contains("&lt;"),
                        "body containing '<' must be HTML-escaped");
            }
        }
    }

    @Test
    void markdown_pipeline_preserves_step_order_under_fuzz() {
        Random r = new Random(7);
        for (int i = 0; i < 50; i++) {
            List<UpgradePlan.Step> steps = new java.util.ArrayList<>();
            int n = 3 + r.nextInt(7);
            for (int j = 0; j < n; j++) {
                steps.add(new UpgradePlan.Step(j + 1,
                        UpgradePlan.StepKind.PRE_CHECK,
                        "step " + j + " " + randomAscii(r, 30),
                        randomAscii(r, 60), null));
            }
            UpgradePlan.Plan plan = new UpgradePlan.Plan(
                    Version.parse("6.0"), Version.parse("7.0"),
                    List.of(), steps);
            String md = renderer.renderMarkdown(plan);
            // Step indices should appear in ascending order.
            int last = 0;
            for (int j = 0; j < n; j++) {
                int idx = md.indexOf((j + 1) + ". **");
                assertTrue(idx > last,
                        "step " + (j + 1) + " must appear after previous");
                last = idx;
            }
        }
    }

    private static String randomAscii(Random r, int maxLen) {
        int len = r.nextInt(maxLen + 1);
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            // Printable ASCII + a few tag-adjacent chars on purpose.
            char c = (char) (0x20 + r.nextInt(0x5f));
            sb.append(c);
        }
        return sb.toString();
    }
}
