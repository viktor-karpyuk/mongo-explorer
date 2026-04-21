package com.kubrik.mex.backup.sink;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;

/**
 * v2.5 STG-1..5 — abstract storage target for backup artefacts.
 *
 * <p>Sealed so the permit list is the authoritative set of supported sink
 * kinds. Local filesystem is the only ready impl in v2.5.0; cloud targets
 * (S3 / GCS / Azure / SFTP) are scaffolded so the sealed hierarchy is
 * complete and {@link com.kubrik.mex.backup.store.SinkRecord#kind()} maps
 * cleanly, but they throw {@link CloudSinkUnavailableException} — real
 * impls land with v2.5.1 once the SDK dependency set is chosen.</p>
 *
 * <p>All I/O methods are blocking and assumed-safe to call off the JavaFX
 * application thread. Callers close {@link InputStream} / {@link OutputStream}
 * instances themselves.</p>
 */
public sealed interface StorageTarget
        permits LocalFsTarget, S3Target, GcsTarget, AzureBlobTarget, SftpTarget {

    /** Probe result from {@link #testWrite()}. */
    record Probe(boolean writable, long latencyMs, Optional<String> error) {}

    /** Directory / object listing entry returned by {@link #list} and {@link #stat}. */
    record Entry(String relPath, long bytes, long mtime) {}

    /**
     * Writes a 1 KB marker under the sink root, reads it back, measures
     * round-trip latency, and cleans up the marker. Used at policy-save time
     * to confirm credentials + connectivity before persisting.
     */
    Probe testWrite();

    OutputStream put(String relPath) throws IOException;
    InputStream  get(String relPath) throws IOException;
    List<Entry>  list(String relPath) throws IOException;
    Entry        stat(String relPath) throws IOException;
    void         delete(String relPath) throws IOException;

    /** Root URI / path as the user sees it (file:// for local, s3:// for S3, …). */
    String canonicalRoot();

    /** {@code true} when the target computes checksums server-side (typically
     *  cloud sinks); local + SFTP return {@code false} and the catalog
     *  verifier reads the file back. */
    default boolean supportsServerSideHash() { return false; }
}
