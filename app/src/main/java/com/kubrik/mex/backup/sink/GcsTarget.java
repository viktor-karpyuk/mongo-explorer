package com.kubrik.mex.backup.sink;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * v2.5 Q2.5-H (stub) — Google Cloud Storage sink. Scaffolded only; every
 * operation throws {@link CloudSinkUnavailableException}. Real impl lands
 * with v2.5.1 once {@code com.google.cloud:google-cloud-storage} is added.
 */
public final class GcsTarget implements StorageTarget {

    private final String name;
    private final String bucketUri;

    public GcsTarget(String name, String bucketUri) {
        this.name = name;
        this.bucketUri = bucketUri;
    }

    public String name() { return name; }

    @Override public Probe testWrite() { throw unavailable(); }
    @Override public OutputStream put(String relPath) { throw unavailable(); }
    @Override public InputStream  get(String relPath) { throw unavailable(); }
    @Override public List<Entry>  list(String relPath) { throw unavailable(); }
    @Override public Entry        stat(String relPath) { throw unavailable(); }
    @Override public void         delete(String relPath) { throw unavailable(); }
    @Override public String canonicalRoot() { return bucketUri; }
    @Override public boolean supportsServerSideHash() { return true; }

    private static CloudSinkUnavailableException unavailable() {
        return new CloudSinkUnavailableException("GCS");
    }
}
