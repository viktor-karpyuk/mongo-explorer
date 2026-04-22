package com.kubrik.mex.backup.sink;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6.1 Q2.6.1-B — Azure Blob URI + credential classifier tests.
 * Live blob operations need a real endpoint and are deferred to a
 * smoke test.
 */
class AzureBlobTargetTest {

    /* =========================== parseUri =========================== */

    @Test
    void parses_azblob_scheme_with_prefix() {
        AzureBlobTarget.Parsed p = AzureBlobTarget.parseUri(
                "azblob://mystorage/my-container/backups");
        assertEquals("mystorage", p.account());
        assertEquals("my-container", p.container());
        assertEquals("backups/", p.keyPrefix());
    }

    @Test
    void parses_azblob_without_prefix() {
        AzureBlobTarget.Parsed p = AzureBlobTarget.parseUri(
                "azblob://mystorage/my-container");
        assertEquals("mystorage", p.account());
        assertEquals("my-container", p.container());
        assertEquals("", p.keyPrefix());
    }

    @Test
    void parses_full_https_blob_url() {
        AzureBlobTarget.Parsed p = AzureBlobTarget.parseUri(
                "https://mystorage.blob.core.windows.net/my-container/a/b");
        assertEquals("mystorage", p.account());
        assertEquals("my-container", p.container());
        assertEquals("a/b/", p.keyPrefix());
    }

    @Test
    void https_URL_without_prefix() {
        AzureBlobTarget.Parsed p = AzureBlobTarget.parseUri(
                "https://mystorage.blob.core.windows.net/my-container");
        assertEquals("mystorage", p.account());
        assertEquals("my-container", p.container());
        assertEquals("", p.keyPrefix());
    }

    @Test
    void rejects_blank_and_null_uri() {
        assertThrows(IllegalArgumentException.class,
                () -> AzureBlobTarget.parseUri(""));
        assertThrows(IllegalArgumentException.class,
                () -> AzureBlobTarget.parseUri(null));
    }

    @Test
    void rejects_wrong_scheme() {
        assertThrows(IllegalArgumentException.class,
                () -> AzureBlobTarget.parseUri("gs://bucket/prefix"));
        assertThrows(IllegalArgumentException.class,
                () -> AzureBlobTarget.parseUri("file:///tmp"));
    }

    @Test
    void rejects_azblob_missing_container() {
        assertThrows(IllegalArgumentException.class,
                () -> AzureBlobTarget.parseUri("azblob://mystorage"));
    }

    @Test
    void rejects_https_missing_container() {
        assertThrows(IllegalArgumentException.class,
                () -> AzureBlobTarget.parseUri(
                        "https://mystorage.blob.core.windows.net"));
    }

    /* ======================== classifyCredentials ======================== */

    @Test
    void anonymous_classification_for_blank_credentials() {
        assertEquals(AzureBlobTarget.AuthKind.ANONYMOUS,
                AzureBlobTarget.classifyCredentials(null));
        assertEquals(AzureBlobTarget.AuthKind.ANONYMOUS,
                AzureBlobTarget.classifyCredentials(""));
        assertEquals(AzureBlobTarget.AuthKind.ANONYMOUS,
                AzureBlobTarget.classifyCredentials("   "));
    }

    @Test
    void SAS_classification() {
        assertEquals(AzureBlobTarget.AuthKind.SAS,
                AzureBlobTarget.classifyCredentials(
                        "{\"sasToken\":\"?sv=2022-11-02&sig=abc\"}"));
    }

    @Test
    void account_key_classification() {
        assertEquals(AzureBlobTarget.AuthKind.ACCOUNT_KEY,
                AzureBlobTarget.classifyCredentials(
                        "{\"accountName\":\"mystorage\",\"accountKey\":\"key=\"}"));
    }

    @Test
    void SAS_wins_over_account_key_when_both_present() {
        // Ambiguous fixture: the builder prefers SAS (cheaper + more
        // granular); the classifier documents the precedence.
        assertEquals(AzureBlobTarget.AuthKind.SAS,
                AzureBlobTarget.classifyCredentials(
                        "{\"sasToken\":\"?sv=abc\",\"accountKey\":\"key=\"}"));
    }

    @Test
    void malformed_json_returns_INVALID() {
        assertEquals(AzureBlobTarget.AuthKind.INVALID,
                AzureBlobTarget.classifyCredentials("not json"));
        assertEquals(AzureBlobTarget.AuthKind.INVALID,
                AzureBlobTarget.classifyCredentials("{\"unknown\":\"field\"}"));
    }
}
