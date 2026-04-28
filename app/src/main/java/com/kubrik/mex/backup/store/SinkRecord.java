package com.kubrik.mex.backup.store;

/**
 * v2.5 STG-1..5 — in-memory view of a {@code storage_sinks} row. The DAO
 * decrypts {@code credentials_enc} into {@link #credentialsJson} on read, so
 * callers never touch ciphertext; equivalently, writes take plain-text
 * credentials and the DAO encrypts before persisting.
 */
public record SinkRecord(
        long id,
        String kind,
        String name,
        String rootPath,
        String credentialsJson,
        String extrasJson,
        long createdAt,
        long updatedAt
) {
    public SinkRecord {
        if (kind == null || kind.isBlank()) throw new IllegalArgumentException("kind");
        if (name == null || name.isBlank()) throw new IllegalArgumentException("name");
        if (rootPath == null) throw new IllegalArgumentException("rootPath");
    }

    public SinkRecord withId(long newId) {
        return new SinkRecord(newId, kind, name, rootPath, credentialsJson,
                extrasJson, createdAt, updatedAt);
    }
}
