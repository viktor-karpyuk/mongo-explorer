package com.kubrik.mex.maint.compact;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 Q2.7-E — Covers the pure {@link ResyncRunner#parseMajor}
 * helper; the live dispatch path is covered by the IT rig.
 */
class ResyncRunnerTest {

    @Test
    void parseMajor_handles_every_version_shape() {
        assertEquals(7, ResyncRunner.parseMajor("7.0.5"));
        assertEquals(6, ResyncRunner.parseMajor("6.0"));
        assertEquals(4, ResyncRunner.parseMajor("4.4.28-rc0"));
        assertEquals(5, ResyncRunner.parseMajor("5.0.0-alpha-123"));
        assertEquals(0, ResyncRunner.parseMajor(""));
        assertEquals(0, ResyncRunner.parseMajor(null));
        assertEquals(0, ResyncRunner.parseMajor("garbage"));
    }
}
