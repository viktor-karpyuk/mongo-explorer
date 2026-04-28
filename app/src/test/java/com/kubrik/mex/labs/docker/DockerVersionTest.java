package com.kubrik.mex.labs.docker;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DockerVersionTest {

    @Test
    void parses_docker_version_one_liner() {
        // `docker --version` format
        DockerVersion v = DockerVersion.parse("Docker version 24.0.7, build afdd53b");
        assertNotNull(v);
        assertEquals(24, v.major());
        assertEquals(0, v.minor());
        assertEquals(7, v.patch());
    }

    @Test
    void parses_format_version_output() {
        // `docker version --format '{{.Client.Version}}'` format
        DockerVersion v = DockerVersion.parse("25.0.3");
        assertEquals(new DockerVersion(25, 0, 3), v);
    }

    @Test
    void parses_two_segment_version_as_patch_zero() {
        DockerVersion v = DockerVersion.parse("24.0");
        assertEquals(new DockerVersion(24, 0, 0), v);
    }

    @Test
    void ignores_suffix_after_semver_digits() {
        // Docker sometimes tacks on vendor suffixes like "-rd1".
        DockerVersion v = DockerVersion.parse("Docker version 24.0.7-rd1, build xyz");
        assertEquals(new DockerVersion(24, 0, 7), v);
    }

    @Test
    void returns_null_on_unparseable() {
        assertNull(DockerVersion.parse(null));
        assertNull(DockerVersion.parse(""));
        assertNull(DockerVersion.parse("no digits here"));
    }

    @Test
    void atLeast_comparison() {
        DockerVersion v = new DockerVersion(24, 0, 7);
        assertTrue(v.atLeast(DockerVersion.MIN_SUPPORTED));
        assertTrue(v.atLeast(new DockerVersion(23, 99, 99)));
        assertFalse(v.atLeast(new DockerVersion(25, 0, 0)));
    }

    @Test
    void MIN_SUPPORTED_is_24_0_0() {
        // Doc contract — LAB-DOCKER-2 specifies compose ls --format
        // json as a required feature, which landed in 24.0.
        assertEquals(new DockerVersion(24, 0, 0), DockerVersion.MIN_SUPPORTED);
    }
}
