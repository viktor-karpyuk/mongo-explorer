package com.kubrik.mex.security.encryption;

import java.util.Objects;

/**
 * v2.6 Q2.6-G1 — per-host encryption-at-rest snapshot. Built from
 * {@code serverStatus.encryptionAtRest} (when present) and
 * {@code getCmdLineOpts.security.enableEncryption} as a fallback.
 *
 * @param host            member host:port (or {@code "<cluster>"} for a
 *                        cluster-aggregate when per-node data is not
 *                        available)
 * @param enabled         whether the storage engine is encrypting data
 *                        at rest
 * @param engine          storage engine name (typically
 *                        {@code "wiredTiger"}); {@code ""} when unknown
 * @param keystore        key management shape — KMIP, VAULT, LOCAL_FILE,
 *                        UNKNOWN
 * @param rotatedAtMs     last key rotation timestamp in epoch-ms if the
 *                        server exposes one; {@code null} otherwise
 * @param cipher          cipher suite (e.g. {@code "AES256-CBC"},
 *                        {@code "AES256-GCM"}); {@code ""} when unknown
 * @param sourceCommand   which server command produced the record
 *                        ({@code "serverStatus"} or
 *                        {@code "getCmdLineOpts"}) — useful for
 *                        the CIS scan explanation drawer.
 */
public record EncryptionStatus(
        String host,
        boolean enabled,
        String engine,
        Keystore keystore,
        Long rotatedAtMs,
        String cipher,
        String sourceCommand
) {

    public enum Keystore { KMIP, VAULT, LOCAL_FILE, UNKNOWN, NONE }

    public EncryptionStatus {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(keystore, "keystore");
        if (engine == null) engine = "";
        if (cipher == null) cipher = "";
        if (sourceCommand == null) sourceCommand = "";
    }
}
