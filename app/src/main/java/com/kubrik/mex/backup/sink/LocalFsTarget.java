package com.kubrik.mex.backup.sink;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * v2.5 STG-3 — local filesystem sink. All relative paths resolve against the
 * sink's root directory; any attempt to escape the root via {@code ..} is
 * rejected with {@link IllegalArgumentException} so a mis-typed
 * {@code relPath} can't silently write elsewhere on the disk.
 *
 * <p>Intermediate directories are created on {@link #put} so callers don't
 * have to script the layout upfront.</p>
 */
public final class LocalFsTarget implements StorageTarget {

    /** Marker-file prefix used by {@link #testWrite()}. UUID suffix avoids
     *  collisions with concurrent probes. Visible so tests can sweep the
     *  sink root for leftover probe files. */
    public static final String PROBE_PREFIX = ".mex-probe-";
    private static final int PROBE_BYTES = 1024;

    private final String name;
    private final Path root;

    public LocalFsTarget(String name, String rootPath) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("name");
        if (rootPath == null || rootPath.isBlank()) throw new IllegalArgumentException("rootPath");
        this.name = name;
        this.root = Paths.get(rootPath).toAbsolutePath().normalize();
    }

    public String name() { return name; }

    /** Absolute root as a {@link Path} — used by runners that need to mount
     *  the sink into a subprocess {@code --out} / {@code --dir} arg without
     *  re-parsing {@link #canonicalRoot()}. */
    public Path rootPath() { return root; }

    @Override
    public Probe testWrite() {
        long start = System.nanoTime();
        Path probe = root.resolve(PROBE_PREFIX + UUID.randomUUID());
        try {
            Files.createDirectories(root);
            byte[] payload = new byte[PROBE_BYTES];
            java.util.Arrays.fill(payload, (byte) 'x');
            Files.write(probe, payload, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            byte[] readBack = Files.readAllBytes(probe);
            if (readBack.length != PROBE_BYTES) {
                return new Probe(false, elapsedMs(start),
                        Optional.of("read-back length mismatch"));
            }
            return new Probe(true, elapsedMs(start), Optional.empty());
        } catch (IOException e) {
            return new Probe(false, elapsedMs(start), Optional.of(e.getMessage()));
        } finally {
            try { Files.deleteIfExists(probe); } catch (IOException ignored) {}
        }
    }

    @Override
    public OutputStream put(String relPath) throws IOException {
        Path target = resolve(relPath);
        Files.createDirectories(target.getParent());
        return Files.newOutputStream(target,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    @Override
    public InputStream get(String relPath) throws IOException {
        return Files.newInputStream(resolve(relPath), StandardOpenOption.READ);
    }

    @Override
    public List<Entry> list(String relPath) throws IOException {
        Path dir = resolve(relPath);
        if (!Files.exists(dir)) return List.of();
        if (!Files.isDirectory(dir)) return List.of(toEntry(dir));
        List<Entry> out = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path child : stream) out.add(toEntry(child));
        }
        out.sort(java.util.Comparator.comparing(Entry::relPath));
        return out;
    }

    @Override
    public Entry stat(String relPath) throws IOException {
        return toEntry(resolve(relPath));
    }

    @Override
    public void delete(String relPath) throws IOException {
        Path target = resolve(relPath);
        if (!Files.exists(target)) return;
        if (Files.isDirectory(target)) {
            try (var stream = Files.walk(target, FileVisitOption.FOLLOW_LINKS)) {
                stream.sorted(java.util.Comparator.reverseOrder())
                        .forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignored) {} });
            }
        } else {
            Files.delete(target);
        }
    }

    @Override
    public String canonicalRoot() { return "file://" + root.toString(); }

    /* ============================ internals ============================ */

    private Path resolve(String relPath) {
        if (relPath == null) throw new IllegalArgumentException("relPath");
        Path abs = root.resolve(relPath).normalize();
        if (!abs.startsWith(root)) {
            throw new IllegalArgumentException("path escapes sink root: " + relPath);
        }
        return abs;
    }

    private Entry toEntry(Path p) throws IOException {
        String rel = root.relativize(p).toString();
        long bytes = Files.isRegularFile(p) ? Files.size(p) : 0L;
        long mtime = Files.getLastModifiedTime(p).toMillis();
        return new Entry(rel, bytes, mtime);
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }
}
