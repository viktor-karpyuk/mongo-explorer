package com.kubrik.mex.k8s.preflight.checks;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ClusterVersionCheckTest {

    @Test
    void parses_standard_git_version() {
        assertEquals("1.30", ClusterVersionCheck.majorDotMinor("v1.30.2"));
    }

    @Test
    void parses_version_without_v_prefix() {
        assertEquals("1.29", ClusterVersionCheck.majorDotMinor("1.29.4"));
    }

    @Test
    void two_part_version_returns_as_is() {
        assertEquals("1.30", ClusterVersionCheck.majorDotMinor("1.30"));
    }

    @Test
    void unparseable_version_returns_null() {
        assertNull(ClusterVersionCheck.majorDotMinor("garbage"));
        assertNull(ClusterVersionCheck.majorDotMinor(null));
    }
}
