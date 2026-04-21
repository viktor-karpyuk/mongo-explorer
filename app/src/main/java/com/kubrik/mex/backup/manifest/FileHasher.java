package com.kubrik.mex.backup.manifest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * v2.5 BKP-RUN-5 — streaming SHA-256 hasher for backup artefacts.
 *
 * <p>Backup files can be gigabytes; hashing via {@code Files.readAllBytes}
 * would blow the heap. {@link #hashFile(Path)} reads through a 64 KB buffer
 * so peak memory stays bounded regardless of file size.</p>
 */
public final class FileHasher {

    private static final int BUFFER_SIZE = 64 * 1024;

    private FileHasher() {}

    public static String hashFile(Path file) throws IOException {
        MessageDigest md = newDigest();
        try (InputStream in = Files.newInputStream(file)) {
            byte[] buf = new byte[BUFFER_SIZE];
            int n;
            while ((n = in.read(buf)) > 0) md.update(buf, 0, n);
        }
        return hex(md.digest());
    }

    public static String hashBytes(byte[] bytes) {
        MessageDigest md = newDigest();
        md.update(bytes);
        return hex(md.digest());
    }

    private static MessageDigest newDigest() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException("SHA-256 unavailable", e); }
    }

    private static String hex(byte[] digest) {
        StringBuilder sb = new StringBuilder(digest.length * 2);
        for (byte b : digest) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
