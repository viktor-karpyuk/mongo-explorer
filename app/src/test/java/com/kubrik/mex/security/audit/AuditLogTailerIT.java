package com.kubrik.mex.security.audit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-C1 — end-to-end tailer test. Appends lines to a file and
 * asserts the parser-produced events reach the sink. Also covers
 * rotate-detection by truncating the file mid-run.
 *
 * <p>Tailer uses a 50 ms poll for test speed; production defaults to 500 ms.</p>
 */
class AuditLogTailerIT {

    @TempDir Path tmp;

    @Test
    void tailer_delivers_appended_events_to_the_sink() throws Exception {
        Path log = tmp.resolve("audit.json");
        Files.writeString(log, "");
        CopyOnWriteArrayList<AuditEvent> captured = new CopyOnWriteArrayList<>();
        CountDownLatch two = new CountDownLatch(2);

        try (AuditLogTailer tailer = new AuditLogTailer(log, e -> {
            captured.add(e);
            two.countDown();
        }, 50L)) {
            tailer.start();

            // Race fence: the tailer's worker thread initialises its
            // offset to the current file size on entry. If the test
            // appends before that init runs, the offset lands past our
            // lines and they're skipped. 100 ms is three poll cycles
            // and leaves room for CI scheduler jitter.
            Thread.sleep(100);

            append(log, "{\"atype\":\"authenticate\",\"ts\":1000,\"users\":[{\"user\":\"dba\",\"db\":\"admin\"}],\"param\":{}}\n");
            append(log, "{\"atype\":\"logout\",\"ts\":2000,\"users\":[{\"user\":\"dba\",\"db\":\"admin\"}],\"param\":{}}\n");

            assertTrue(two.await(10, TimeUnit.SECONDS),
                    "tailer must deliver both appended events within 10 s");
        }

        assertEquals(2, captured.size());
        assertEquals("authenticate", captured.get(0).atype());
        assertEquals("logout", captured.get(1).atype());
    }

    @Test
    void tailer_reopens_after_file_rotation() throws Exception {
        Path log = tmp.resolve("audit.json");
        Files.writeString(log, "");
        CopyOnWriteArrayList<AuditEvent> captured = new CopyOnWriteArrayList<>();
        CountDownLatch afterRotate = new CountDownLatch(1);

        try (AuditLogTailer tailer = new AuditLogTailer(log, e -> {
            captured.add(e);
            if ("createUser".equals(e.atype())) afterRotate.countDown();
        }, 50L)) {
            tailer.start();
            Thread.sleep(100);  // let the worker init its offset

            append(log, "{\"atype\":\"authenticate\",\"ts\":1,\"users\":[],\"param\":{}}\n");
            // Rotation: truncate the file and write a fresh entry. Real
            // log-rotate renames then creates; file-shrink detection is
            // the cross-platform fallback the tailer relies on.
            Thread.sleep(150);  // let the tailer consume the first line
            Files.writeString(log, "", StandardOpenOption.TRUNCATE_EXISTING);
            append(log, "{\"atype\":\"createUser\",\"ts\":2,\"users\":[],\"param\":{}}\n");

            assertTrue(afterRotate.await(5, TimeUnit.SECONDS),
                    "tailer must detect the rotation and pick up the new entry");
        }

        assertTrue(captured.stream().anyMatch(e -> e.atype().equals("createUser")),
                "createUser event must appear after rotation");
    }

    @Test
    void malformed_lines_are_skipped_silently() throws Exception {
        Path log = tmp.resolve("audit.json");
        Files.writeString(log, "");
        CopyOnWriteArrayList<AuditEvent> captured = new CopyOnWriteArrayList<>();
        CountDownLatch one = new CountDownLatch(1);

        try (AuditLogTailer tailer = new AuditLogTailer(log, e -> {
            captured.add(e);
            one.countDown();
        }, 50L)) {
            tailer.start();
            Thread.sleep(100);  // let the worker init its offset

            append(log, "{not json\n");
            append(log, "\n");                               // blank line
            append(log, "{\"missing\":\"atype\"}\n");         // no atype
            append(log, "{\"atype\":\"authenticate\",\"ts\":1,\"users\":[],\"param\":{}}\n");

            assertTrue(one.await(5, TimeUnit.SECONDS));
        }

        assertEquals(1, captured.size(), "only the well-formed line should land in the sink");
    }

    private static void append(Path p, String s) throws Exception {
        Files.writeString(p, s, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
