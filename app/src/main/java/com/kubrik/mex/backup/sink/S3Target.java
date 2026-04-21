package com.kubrik.mex.backup.sink;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;

/**
 * v2.5 Q2.5-H (stub) — S3 object-store sink. Ships scaffolded so
 * {@link StorageTarget}'s sealed permit list is complete and the app can
 * persist S3 sink records via {@link com.kubrik.mex.backup.store.SinkDao},
 * but every operation throws {@link CloudSinkUnavailableException}. Real
 * S3 support lands with v2.5.1 once the AWS SDK dependency is pinned.
 */
public final class S3Target implements StorageTarget {

    private final String name;
    private final String bucketUri;
    private final String region;

    public S3Target(String name, String bucketUri, String region) {
        this.name = name;
        this.bucketUri = bucketUri;
        this.region = region;
    }

    public String name() { return name; }
    public String region() { return region; }

    @Override public Probe testWrite() { throw unavailable(); }
    @Override public OutputStream put(String relPath) { throw unavailable(); }
    @Override public InputStream  get(String relPath) { throw unavailable(); }
    @Override public List<Entry>  list(String relPath) { throw unavailable(); }
    @Override public Entry        stat(String relPath) { throw unavailable(); }
    @Override public void         delete(String relPath) { throw unavailable(); }
    @Override public String canonicalRoot() { return bucketUri; }
    @Override public boolean supportsServerSideHash() { return true; }

    private static CloudSinkUnavailableException unavailable() {
        return new CloudSinkUnavailableException("S3");
    }
}
