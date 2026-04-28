package com.kubrik.mex.backup;

import com.kubrik.mex.backup.runner.OutputDirTemplate;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.5 BKP-POLICY-6 — placeholder substitution for the output-dir template.
 * Verifies timestamp formatting, filesystem-safe character scrubbing, and
 * multi-placeholder templates.
 */
class OutputDirTemplateTest {

    private static final Instant WHEN = Instant.parse("2026-04-21T03:15:00Z");

    @Test
    void default_template_renders_policy_and_utc_timestamp() {
        String out = OutputDirTemplate.render("<policy>/<yyyy-MM-dd_HH-mm-ss>",
                "nightly-reports", "cx-1", WHEN);
        assertEquals("nightly-reports/2026-04-21_03-15-00", out);
    }

    @Test
    void date_only_placeholder_drops_clock_detail() {
        assertEquals("nightly/2026-04-21",
                OutputDirTemplate.render("<policy>/<yyyy-MM-dd>", "nightly", "cx", WHEN));
    }

    @Test
    void unsafe_characters_in_policy_name_are_replaced_with_dashes() {
        String out = OutputDirTemplate.render("<policy>/run",
                "nightly/weekly+reports", "cx", WHEN);
        assertEquals("nightly-weekly-reports/run", out);
    }

    @Test
    void connection_placeholder_expands() {
        String out = OutputDirTemplate.render("<connection>/<yyyy-MM-dd_HH-mm-ss>",
                "p", "prod-east", WHEN);
        assertEquals("prod-east/2026-04-21_03-15-00", out);
    }

    @Test
    void null_template_falls_back_to_default() {
        String out = OutputDirTemplate.render(null, "p", "cx", WHEN);
        assertEquals("p/2026-04-21_03-15-00", out);
    }
}
