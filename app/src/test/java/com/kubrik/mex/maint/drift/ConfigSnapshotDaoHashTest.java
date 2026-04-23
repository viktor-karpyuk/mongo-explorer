package com.kubrik.mex.maint.drift;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 DRIFT-CFG — direct coverage for the static
 * {@link ConfigSnapshotDao#sha256} helper so a hash-algorithm change
 * would fail tests instead of silently altering stored hashes.
 */
class ConfigSnapshotDaoHashTest {

    @Test
    void sha256_matches_known_vector() {
        // echo -n "" | shasum -a 256
        assertEquals(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                ConfigSnapshotDao.sha256(""));
    }

    @Test
    void sha256_is_stable_and_case_consistent() {
        String h1 = ConfigSnapshotDao.sha256("{\"foo\":1}");
        String h2 = ConfigSnapshotDao.sha256("{\"foo\":1}");
        assertEquals(h1, h2);
        // Output is lowercase hex; 64 chars.
        assertEquals(64, h1.length());
        assertEquals(h1, h1.toLowerCase());
    }

    @Test
    void sha256_differs_on_any_byte_change() {
        String h1 = ConfigSnapshotDao.sha256("abc");
        String h2 = ConfigSnapshotDao.sha256("abd");
        assertNotEquals(h1, h2);
    }
}
