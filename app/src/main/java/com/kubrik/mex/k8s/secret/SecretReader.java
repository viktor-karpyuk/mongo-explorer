package com.kubrik.mex.k8s.secret;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Secret;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-B2 — Small helper wrapping {@link CoreV1Api} Secret
 * reads with the conventions the operator resolvers need.
 *
 * <p>Keeps the resolvers free of plumbing: they ask for a named
 * Secret and get {@link Optional} data back. A missing Secret surface
 * (404) returns {@link Optional#empty()} — silence is a valid signal
 * here, because not every operator deployment has every convention
 * Secret. Other API errors bubble as {@link ApiException} so the
 * service layer can aggregate them into a {@code DiscoveryEvent.Failed}.</p>
 */
public final class SecretReader {

    private static final HexFormat HEX = HexFormat.of();

    private final ApiClient client;

    public SecretReader(ApiClient client) {
        this.client = client;
    }

    public Optional<V1Secret> read(String namespace, String name) throws ApiException {
        try {
            V1Secret s = new CoreV1Api(client).readNamespacedSecret(name, namespace).execute();
            return Optional.ofNullable(s);
        } catch (ApiException ae) {
            if (ae.getCode() == 404) return Optional.empty();
            throw ae;
        }
    }

    /**
     * Pull a string value from a Secret, checking both {@code data}
     * (base64-encoded bytes; we always decode as UTF-8) and {@code
     * stringData} (for freshly-created Secrets that haven't round-
     * tripped through the server yet).
     */
    public static Optional<String> stringValue(V1Secret s, String key) {
        if (s == null) return Optional.empty();
        Map<String, byte[]> data = s.getData();
        if (data != null && data.containsKey(key) && data.get(key) != null) {
            return Optional.of(new String(data.get(key), StandardCharsets.UTF_8));
        }
        Map<String, String> strData = s.getStringData();
        if (strData != null && strData.get(key) != null) {
            return Optional.of(strData.get(key));
        }
        return Optional.empty();
    }

    public static Optional<byte[]> byteValue(V1Secret s, String key) {
        if (s == null) return Optional.empty();
        Map<String, byte[]> data = s.getData();
        if (data != null && data.get(key) != null) return Optional.of(data.get(key));
        Map<String, String> strData = s.getStringData();
        if (strData != null && strData.get(key) != null) {
            return Optional.of(strData.get(key).getBytes(StandardCharsets.UTF_8));
        }
        return Optional.empty();
    }

    /** SHA-256 hex digest of {@code bytes}, or empty when bytes is null. */
    public static Optional<String> fingerprint(byte[] bytes) {
        if (bytes == null) return Optional.empty();
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return Optional.of(HEX.formatHex(md.digest(bytes)));
        } catch (NoSuchAlgorithmException nsa) {
            // SHA-256 is part of the JRE's mandatory algorithm set — so
            // this can't actually happen, but rethrowing as unchecked
            // keeps the resolver call site clean.
            throw new IllegalStateException("SHA-256 unavailable", nsa);
        }
    }
}
