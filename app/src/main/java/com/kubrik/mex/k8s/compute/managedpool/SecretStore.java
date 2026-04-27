package com.kubrik.mex.k8s.compute.managedpool;

import java.util.Optional;

/**
 * v2.8.4 Q2.8.4-A — Pluggable backing store for cloud-credential
 * secret material.
 *
 * <p>Production wires {@link OsKeychainSecretStore} which routes to
 * the macOS Keychain / Windows Credential Manager / Linux libsecret
 * via {@code security}, {@code cmdkey}, and {@code secret-tool} CLIs
 * respectively — keeping native bindings out of the app image.
 * Tests can swap in {@link InMemorySecretStore}.</p>
 *
 * <p>The {@link CloudCredential#keychainRef()} string is the only
 * thing Mongo Explorer's SQLite holds; this interface is the
 * sole place that handles the actual secret.</p>
 */
public interface SecretStore {

    /** Persist {@code secret} keyed by {@code ref}. Overwrites a
     *  previous value at the same ref. */
    void store(String ref, String secret);

    /** Read the secret. Empty if no row matches the ref. */
    Optional<String> read(String ref);

    /** Delete the secret. No-op if it didn't exist. */
    void delete(String ref);

    /** Build a fresh ref for a credential. The format is opaque to
     *  callers — they treat it as a UUID-shaped opaque token. The
     *  full UUID (128 bits) is appended so the birthday collision
     *  window is astronomical even for organisations with millions
     *  of credentials. */
    static String newRef(CloudProvider provider, String displayName) {
        String safe = displayName.toLowerCase().replaceAll("[^a-z0-9]", "-")
                .replaceAll("-+", "-");
        return "mex/" + provider.wireValue().toLowerCase() + "/" + safe + "-"
                + java.util.UUID.randomUUID();
    }
}
