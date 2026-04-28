package com.kubrik.mex.k8s.model;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-B2 — In-memory credentials + TLS material resolved
 * from Kubernetes Secrets for a discovered Mongo workload.
 *
 * <p>By design this record lives only on the heap. We never write
 * raw passwords / keys / CA bytes to SQLite; the {@code
 * tlsCaFingerprint} field carries a SHA-256 hex digest instead, and
 * that is what the connection row persists for audit.</p>
 *
 * <p>Every field is {@link Optional} because pickup is best-effort
 * and tolerant of missing Secrets: the user sees whatever was
 * resolved and can fill in the rest manually.</p>
 */
public record MongoCredentials(
        Optional<String> username,
        Optional<String> password,
        Optional<String> authDatabase,
        Optional<byte[]> tlsCaPem,
        Optional<String> tlsCaFingerprint,
        Optional<byte[]> tlsClientCertPem,
        Optional<byte[]> tlsClientKeyPem,
        Optional<String> srvOverride) {

    public MongoCredentials {
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(password, "password");
        Objects.requireNonNull(authDatabase, "authDatabase");
        Objects.requireNonNull(tlsCaPem, "tlsCaPem");
        Objects.requireNonNull(tlsCaFingerprint, "tlsCaFingerprint");
        Objects.requireNonNull(tlsClientCertPem, "tlsClientCertPem");
        Objects.requireNonNull(tlsClientKeyPem, "tlsClientKeyPem");
        Objects.requireNonNull(srvOverride, "srvOverride");
    }

    public static MongoCredentials empty() {
        return new MongoCredentials(
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(),
                Optional.empty());
    }

    public boolean hasCredentials() {
        return username.isPresent() && password.isPresent();
    }

    public boolean hasTlsMaterial() {
        return tlsCaPem.isPresent() || tlsClientCertPem.isPresent();
    }
}
