package com.kubrik.mex.backup.sink;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-L1 — S3 URI parsing. Live S3 round-trips need credentials
 * + network; an IT harness against localstack lands with Q2.6-K (if
 * the project opts to pull testcontainers-localstack, ~200 MB). The
 * decidable slice — URI → (bucket, keyPrefix) — is unit-covered here.
 */
class S3TargetTest {

    @Test
    void parses_bucket_only_uri() {
        S3Target.Parsed p = S3Target.parseBucketUri("s3://my-bucket");
        assertEquals("my-bucket", p.bucket());
        assertEquals("", p.keyPrefix());
    }

    @Test
    void parses_bucket_with_single_segment_prefix() {
        S3Target.Parsed p = S3Target.parseBucketUri("s3://my-bucket/backups");
        assertEquals("my-bucket", p.bucket());
        assertEquals("backups/", p.keyPrefix(),
                "trailing slash is normalised on so resolveKey can concat cleanly");
    }

    @Test
    void parses_bucket_with_multi_segment_prefix() {
        S3Target.Parsed p = S3Target.parseBucketUri("s3://my-bucket/a/b/c");
        assertEquals("my-bucket", p.bucket());
        assertEquals("a/b/c/", p.keyPrefix());
    }

    @Test
    void preserves_trailing_slash_when_already_present() {
        S3Target.Parsed p = S3Target.parseBucketUri("s3://my-bucket/backups/");
        assertEquals("my-bucket", p.bucket());
        assertEquals("backups/", p.keyPrefix(),
                "already-trailing-slash prefix should not double-slash");
    }

    @Test
    void normalises_multiple_leading_slashes_after_scheme() {
        S3Target.Parsed p = S3Target.parseBucketUri("s3:///my-bucket//a");
        assertEquals("my-bucket", p.bucket());
        assertEquals("a/", p.keyPrefix());
    }

    @Test
    void accepts_uppercase_scheme() {
        S3Target.Parsed p = S3Target.parseBucketUri("S3://my-bucket");
        assertEquals("my-bucket", p.bucket());
    }

    @Test
    void rejects_blank_uri() {
        assertThrows(IllegalArgumentException.class,
                () -> S3Target.parseBucketUri(""));
        assertThrows(IllegalArgumentException.class,
                () -> S3Target.parseBucketUri(null));
        assertThrows(IllegalArgumentException.class,
                () -> S3Target.parseBucketUri("   "));
    }

    @Test
    void rejects_wrong_scheme() {
        assertThrows(IllegalArgumentException.class,
                () -> S3Target.parseBucketUri("gs://my-bucket"));
        assertThrows(IllegalArgumentException.class,
                () -> S3Target.parseBucketUri("file:///tmp"));
    }

    @Test
    void rejects_scheme_only_with_no_bucket() {
        assertThrows(IllegalArgumentException.class,
                () -> S3Target.parseBucketUri("s3://"));
        assertThrows(IllegalArgumentException.class,
                () -> S3Target.parseBucketUri("s3:///"));
        assertThrows(IllegalArgumentException.class,
                () -> S3Target.parseBucketUri("s3:///////"));
    }

    @Test
    void strips_query_string_from_pasted_browser_url() {
        // Pasted from the AWS console with a versioning query param.
        // Without the strip the '?version=123' landed inside the key
        // prefix and every later PUT failed with an opaque InvalidKey.
        S3Target.Parsed p = S3Target.parseBucketUri(
                "s3://my-bucket/daily?version=123");
        assertEquals("my-bucket", p.bucket());
        assertEquals("daily/", p.keyPrefix());
    }

    @Test
    void strips_fragment_from_pasted_url() {
        S3Target.Parsed p = S3Target.parseBucketUri(
                "s3://my-bucket/daily#sectionRef");
        assertEquals("daily/", p.keyPrefix());
    }

    @Test
    void strips_whichever_of_query_or_fragment_comes_first() {
        // Either order is fine — we cut at whichever appears earliest.
        S3Target.Parsed p1 = S3Target.parseBucketUri(
                "s3://my-bucket/daily?v=1#anchor");
        assertEquals("daily/", p1.keyPrefix());

        S3Target.Parsed p2 = S3Target.parseBucketUri(
                "s3://my-bucket/daily#anchor?v=1");
        assertEquals("daily/", p2.keyPrefix());
    }
}
