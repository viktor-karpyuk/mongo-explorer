package com.kubrik.mex.k8s.preflight;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PreflightResultTest {

    @Test
    void pass_has_no_message_or_hint() {
        PreflightResult r = PreflightResult.pass("x");
        assertEquals(PreflightResult.Status.PASS, r.status());
        assertTrue(r.message().isEmpty());
        assertTrue(r.hint().isEmpty());
        assertFalse(r.skipped());
    }

    @Test
    void warn_carries_message_and_optional_hint() {
        PreflightResult r = PreflightResult.warn("x", "reason", "hint");
        assertEquals(PreflightResult.Status.WARN, r.status());
        assertEquals("reason", r.message().orElseThrow());
        assertEquals("hint", r.hint().orElseThrow());
    }

    @Test
    void fail_carries_message_and_hint() {
        PreflightResult r = PreflightResult.fail("x", "nope", null);
        assertEquals(PreflightResult.Status.FAIL, r.status());
        assertTrue(r.hint().isEmpty());
    }

    @Test
    void skipped_uses_pass_level_under_the_hood() {
        PreflightResult r = PreflightResult.skipped("x", "n/a");
        assertEquals(PreflightResult.Status.PASS, r.status());
        assertTrue(r.skipped());
        assertEquals("n/a", r.message().orElseThrow());
    }
}
