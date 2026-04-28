package com.kubrik.mex.k8s.secret;

import io.kubernetes.client.openapi.models.V1Secret;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SecretReaderTest {

    @Test
    void reads_data_bytes_as_utf8_string() {
        V1Secret s = new V1Secret().data(Map.of(
                "password", "p@ssw0rd".getBytes(StandardCharsets.UTF_8)));
        assertEquals("p@ssw0rd", SecretReader.stringValue(s, "password").orElse(null));
    }

    @Test
    void falls_back_to_stringData() {
        V1Secret s = new V1Secret();
        Map<String, String> strData = new HashMap<>();
        strData.put("username", "alice");
        s.stringData(strData);
        assertEquals("alice", SecretReader.stringValue(s, "username").orElse(null));
    }

    @Test
    void missing_key_yields_empty() {
        V1Secret s = new V1Secret().data(Map.of("other", new byte[] {1, 2}));
        assertTrue(SecretReader.stringValue(s, "missing").isEmpty());
    }

    @Test
    void null_secret_yields_empty() {
        assertTrue(SecretReader.stringValue(null, "x").isEmpty());
        assertTrue(SecretReader.byteValue(null, "x").isEmpty());
    }

    @Test
    void fingerprint_is_sha256_hex() {
        // SHA-256 of "hello" = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
        String digest = SecretReader.fingerprint("hello".getBytes(StandardCharsets.UTF_8))
                .orElseThrow();
        assertEquals("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", digest);
    }

    @Test
    void fingerprint_absent_for_null() {
        assertTrue(SecretReader.fingerprint(null).isEmpty());
    }
}
