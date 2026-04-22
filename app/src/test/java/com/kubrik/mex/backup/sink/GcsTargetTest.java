package com.kubrik.mex.backup.sink;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6.1 Q2.6.1-A — GCS URI parsing. Live GCS round-trips need
 * credentials + network and are deferred to a smoke test against a
 * real bucket. The decidable slice — URI → (bucket, keyPrefix) — is
 * unit-covered here.
 */
class GcsTargetTest {

    @Test
    void parses_bucket_only_uri() {
        GcsTarget.Parsed p = GcsTarget.parseBucketUri("gs://my-bucket");
        assertEquals("my-bucket", p.bucket());
        assertEquals("", p.keyPrefix());
    }

    @Test
    void parses_bucket_with_single_segment_prefix() {
        GcsTarget.Parsed p = GcsTarget.parseBucketUri("gs://my-bucket/backups");
        assertEquals("my-bucket", p.bucket());
        assertEquals("backups/", p.keyPrefix(),
                "trailing slash normalised on so resolveKey can concat cleanly");
    }

    @Test
    void parses_bucket_with_multi_segment_prefix() {
        GcsTarget.Parsed p = GcsTarget.parseBucketUri("gs://my-bucket/a/b/c");
        assertEquals("my-bucket", p.bucket());
        assertEquals("a/b/c/", p.keyPrefix());
    }

    @Test
    void preserves_trailing_slash_when_already_present() {
        GcsTarget.Parsed p = GcsTarget.parseBucketUri("gs://my-bucket/backups/");
        assertEquals("my-bucket", p.bucket());
        assertEquals("backups/", p.keyPrefix());
    }

    @Test
    void accepts_uppercase_scheme() {
        GcsTarget.Parsed p = GcsTarget.parseBucketUri("GS://my-bucket");
        assertEquals("my-bucket", p.bucket());
    }

    @Test
    void rejects_blank_uri() {
        assertThrows(IllegalArgumentException.class,
                () -> GcsTarget.parseBucketUri(""));
        assertThrows(IllegalArgumentException.class,
                () -> GcsTarget.parseBucketUri(null));
    }

    @Test
    void rejects_wrong_scheme() {
        assertThrows(IllegalArgumentException.class,
                () -> GcsTarget.parseBucketUri("s3://my-bucket"));
        assertThrows(IllegalArgumentException.class,
                () -> GcsTarget.parseBucketUri("file:///tmp"));
    }

    @Test
    void rejects_scheme_without_bucket() {
        assertThrows(IllegalArgumentException.class,
                () -> GcsTarget.parseBucketUri("gs://"));
        assertThrows(IllegalArgumentException.class,
                () -> GcsTarget.parseBucketUri("gs:///"));
    }
}
