package com.kubrik.mex.backup.sink;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * v2.5 Q2.5-H (stub) — Azure Blob Storage sink. Scaffolded only; every
 * operation throws {@link CloudSinkUnavailableException}. Real impl lands
 * with v2.5.1 once {@code com.azure:azure-storage-blob} is added.
 */
public final class AzureBlobTarget implements StorageTarget {

    private final String name;
    private final String containerUri;

    public AzureBlobTarget(String name, String containerUri) {
        this.name = name;
        this.containerUri = containerUri;
    }

    public String name() { return name; }

    @Override public Probe testWrite() { throw unavailable(); }
    @Override public OutputStream put(String relPath) { throw unavailable(); }
    @Override public InputStream  get(String relPath) { throw unavailable(); }
    @Override public List<Entry>  list(String relPath) { throw unavailable(); }
    @Override public Entry        stat(String relPath) { throw unavailable(); }
    @Override public void         delete(String relPath) { throw unavailable(); }
    @Override public String canonicalRoot() { return containerUri; }
    @Override public boolean supportsServerSideHash() { return true; }

    private static CloudSinkUnavailableException unavailable() {
        return new CloudSinkUnavailableException("Azure");
    }
}
