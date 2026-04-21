package com.kubrik.mex.backup.sink;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * v2.5 Q2.5-H (stub) — SFTP sink. Scaffolded only; every operation throws
 * {@link CloudSinkUnavailableException}. Real impl lands with v2.5.1 once
 * an SSH library (JSch / Apache MINA SSHD) is chosen + added to the build.
 */
public final class SftpTarget implements StorageTarget {

    private final String name;
    private final String sftpUri;

    public SftpTarget(String name, String sftpUri) {
        this.name = name;
        this.sftpUri = sftpUri;
    }

    public String name() { return name; }

    @Override public Probe testWrite() { throw unavailable(); }
    @Override public OutputStream put(String relPath) { throw unavailable(); }
    @Override public InputStream  get(String relPath) { throw unavailable(); }
    @Override public List<Entry>  list(String relPath) { throw unavailable(); }
    @Override public Entry        stat(String relPath) { throw unavailable(); }
    @Override public void         delete(String relPath) { throw unavailable(); }
    @Override public String canonicalRoot() { return sftpUri; }
    @Override public boolean supportsServerSideHash() { return false; }

    private static CloudSinkUnavailableException unavailable() {
        return new CloudSinkUnavailableException("SFTP");
    }
}
