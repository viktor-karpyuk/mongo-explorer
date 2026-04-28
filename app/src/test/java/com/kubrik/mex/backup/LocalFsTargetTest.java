package com.kubrik.mex.backup;

import com.kubrik.mex.backup.sink.LocalFsTarget;
import com.kubrik.mex.backup.sink.StorageTarget;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.5 STG-1..3 — happy-path round-trip for the local-filesystem sink:
 * write / read / list / stat / delete, probe latency, and the path-escape
 * guard that prevents {@code ..} relative paths from leaving the sink root.
 */
class LocalFsTargetTest {

    @TempDir Path tmp;

    @Test
    void roundTrip_write_read_list_stat_delete() throws IOException {
        StorageTarget sink = new LocalFsTarget("local", tmp.toString());
        byte[] payload = "hello v2.5".getBytes(StandardCharsets.UTF_8);

        try (OutputStream out = sink.put("backups/2026-04-20/users.bson")) {
            out.write(payload);
        }

        try (InputStream in = sink.get("backups/2026-04-20/users.bson")) {
            byte[] back = in.readAllBytes();
            assertArrayEquals(payload, back);
        }

        StorageTarget.Entry stat = sink.stat("backups/2026-04-20/users.bson");
        assertEquals(payload.length, stat.bytes());
        assertTrue(stat.mtime() > 0);

        List<StorageTarget.Entry> list = sink.list("backups/2026-04-20");
        assertEquals(1, list.size());
        assertEquals("backups/2026-04-20/users.bson", list.get(0).relPath());

        sink.delete("backups/2026-04-20/users.bson");
        assertFalse(Files.exists(tmp.resolve("backups/2026-04-20/users.bson")));
    }

    @Test
    void testWrite_probe_is_clean() {
        LocalFsTarget sink = new LocalFsTarget("local", tmp.toString());
        StorageTarget.Probe probe = sink.testWrite();
        assertTrue(probe.writable(), "probe should report writable on a temp dir");
        assertTrue(probe.latencyMs() >= 0);
        assertTrue(probe.error().isEmpty());
        // No marker files left behind — the probe cleans up regardless of outcome.
        try (Stream<Path> stream = Files.list(tmp)) {
            long leftover = stream.filter(p ->
                    p.getFileName().toString().startsWith(LocalFsTarget.PROBE_PREFIX)).count();
            assertEquals(0, leftover, "probe file must be deleted");
        } catch (IOException e) {
            fail(e);
        }
    }

    @Test
    void testWrite_reports_error_when_root_is_unwritable() throws IOException {
        Path readOnly = tmp.resolve("ro");
        Files.createDirectories(readOnly);
        // Best-effort: mark the directory read-only. Skip the assertion if the
        // platform refuses (Windows, SELinux-hardened containers) — the
        // happy-path test above is the load-bearing coverage.
        java.io.File asFile = readOnly.toFile();
        boolean flipped = asFile.setWritable(false) && asFile.setReadable(true);
        if (!flipped) return;

        LocalFsTarget sink = new LocalFsTarget("local", readOnly.toString());
        StorageTarget.Probe probe = sink.testWrite();
        assertFalse(probe.writable(), "read-only dir must report not writable");
        assertTrue(probe.error().isPresent());

        asFile.setWritable(true);
    }

    @Test
    void resolve_rejects_dot_dot_escape() {
        LocalFsTarget sink = new LocalFsTarget("local", tmp.toString());
        assertThrows(IllegalArgumentException.class,
                () -> sink.stat("../../etc/passwd"));
    }

    @Test
    void canonicalRoot_is_file_uri() {
        LocalFsTarget sink = new LocalFsTarget("local", tmp.toString());
        String canonical = sink.canonicalRoot();
        assertTrue(canonical.startsWith("file://"));
        assertTrue(canonical.endsWith(tmp.toAbsolutePath().normalize().toString()));
    }

    @Test
    void delete_on_directory_recursively_removes_children() throws IOException {
        StorageTarget sink = new LocalFsTarget("local", tmp.toString());
        try (OutputStream a = sink.put("dir/a.txt")) { a.write(new byte[] {1}); }
        try (OutputStream b = sink.put("dir/sub/b.txt")) { b.write(new byte[] {2}); }
        sink.delete("dir");
        assertFalse(Files.exists(tmp.resolve("dir")));
    }

    // Unused import guard so Arrays stays imported when more tests land.
    @SuppressWarnings("unused")
    private static void _keepImport() { Arrays.sort(new int[0]); }
}
