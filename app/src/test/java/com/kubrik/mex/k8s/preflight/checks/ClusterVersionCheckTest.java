package com.kubrik.mex.k8s.preflight.checks;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

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

    // v2.8.1 Q2.8-N — blessed-matrix smoke parametrisation.
    // Each row is a minor-version string + the expected classification
    // so the whole matrix is auditable from a single test file.

    @ParameterizedTest
    @ValueSource(strings = {"1.29", "1.30", "1.31"})
    void blessed_minors_classify_as_blessed(String minor) {
        assertEquals(ClusterVersionCheck.Classification.BLESSED,
                ClusterVersionCheck.classify(minor),
                minor + " must be in the blessed matrix");
    }

    @ParameterizedTest
    @ValueSource(strings = {"1.27", "1.28"})
    void hard_min_minors_classify_as_below_min(String minor) {
        assertEquals(ClusterVersionCheck.Classification.BELOW_MIN,
                ClusterVersionCheck.classify(minor));
    }

    @ParameterizedTest
    @ValueSource(strings = {"1.25", "1.26", "1.32", "1.33", "2.0"})
    void off_matrix_minors_classify_as_outside(String minor) {
        assertEquals(ClusterVersionCheck.Classification.OUTSIDE,
                ClusterVersionCheck.classify(minor));
    }

    @Test
    void null_minor_classifies_as_unparseable() {
        assertEquals(ClusterVersionCheck.Classification.UNPARSEABLE,
                ClusterVersionCheck.classify(null));
    }

    @ParameterizedTest
    @CsvSource({
            "v1.29.7,  1.29",
            "v1.30.0,  1.30",
            "v1.31.2,  1.31",
            "1.29.14,  1.29",
            "1.30,     1.30",
    })
    void end_to_end_parse_then_classify_is_blessed(String git, String expectedMinor) {
        String minor = ClusterVersionCheck.majorDotMinor(git);
        assertEquals(expectedMinor, minor);
        assertEquals(ClusterVersionCheck.Classification.BLESSED,
                ClusterVersionCheck.classify(minor));
    }
}
