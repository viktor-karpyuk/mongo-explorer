package com.kubrik.mex.k8s.preflight;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PreflightSummaryTest {

    @Test
    void has_any_fail_flags_single_fail() {
        PreflightSummary s = new PreflightSummary(List.of(
                PreflightResult.pass("a"),
                PreflightResult.warn("b", "warn", "hint"),
                PreflightResult.fail("c", "fail", "hint")));
        assertTrue(s.hasAnyFail());
        assertEquals(1, s.failing().size());
    }

    @Test
    void warns_to_ack_excludes_skipped() {
        PreflightSummary s = new PreflightSummary(List.of(
                PreflightResult.warn("a", "w1", null),
                PreflightResult.skipped("b", "n/a"),
                PreflightResult.warn("c", "w2", null)));
        assertEquals(2, s.warnsToAck().size());
        assertEquals(1, s.skipped().size());
    }

    @Test
    void passing_excludes_warn_fail_skipped() {
        PreflightSummary s = new PreflightSummary(List.of(
                PreflightResult.pass("a"),
                PreflightResult.warn("b", "w", null),
                PreflightResult.fail("c", "f", null),
                PreflightResult.skipped("d", "n/a")));
        assertEquals(1, s.passing().size());
        assertEquals(4, s.total());
    }

    @Test
    void empty_summary_has_no_fails() {
        PreflightSummary empty = new PreflightSummary(List.of());
        assertFalse(empty.hasAnyFail());
        assertTrue(empty.warnsToAck().isEmpty());
    }
}
