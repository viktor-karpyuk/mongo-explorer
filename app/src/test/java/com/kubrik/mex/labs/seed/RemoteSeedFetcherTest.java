package com.kubrik.mex.labs.seed;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RemoteSeedFetcherTest {

    @Test
    void sha256_of_empty_file_is_known_vector(@TempDir Path tmp) throws Exception {
        Path p = Files.createFile(tmp.resolve("empty"));
        assertEquals(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                RemoteSeedFetcher.sha256Hex(p));
    }

    @Test
    void sha256_of_abc_file_is_known_vector(@TempDir Path tmp) throws Exception {
        Path p = tmp.resolve("abc");
        Files.writeString(p, "abc");
        assertEquals(
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                RemoteSeedFetcher.sha256Hex(p));
    }

    @Test
    void filenameFromUrl_sanitises_url_characters() {
        assertEquals("sampledata.archive",
                RemoteSeedFetcher.filenameFromUrl(
                        "https://atlas-education.s3.amazonaws.com/sampledata.archive"));
        // Query strings dropped.
        assertEquals("archive.tar.gz",
                RemoteSeedFetcher.filenameFromUrl(
                        "https://example.com/path/archive.tar.gz?sig=abc"));
        // Unsafe chars turned into underscores.
        String sanitized = RemoteSeedFetcher.filenameFromUrl(
                "https://example.com/sha:/weird name@1.2.archive");
        assertFalse(sanitized.contains(" "));
        assertFalse(sanitized.contains(":"));
        assertFalse(sanitized.contains("@"));
    }
}
