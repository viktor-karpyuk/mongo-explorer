package com.kubrik.mex.backup;

import com.kubrik.mex.backup.runner.MongodumpCommandBuilder;
import com.kubrik.mex.backup.runner.MongodumpOptions;
import com.kubrik.mex.backup.runner.ProgressLine;
import com.kubrik.mex.backup.runner.RunLog;
import com.kubrik.mex.backup.spec.ArchiveSpec;
import com.kubrik.mex.backup.spec.Scope;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.5 BKP-RUN-1..8 — unit-level coverage for the runner primitives. The
 * end-to-end subprocess round-trip lives in a separate IT (Q2.5-B.4 / §5
 * RunnerIT) that shells out to the real mongodump binary.
 */
class RunnerUnitTest {

    @TempDir Path tmp;

    /* ========================== command builder ========================== */

    @Test
    void whole_cluster_builds_argv_without_db_flags() {
        MongodumpOptions o = new MongodumpOptions(
                "mongodb://user:pass@h1/?replicaSet=rs0",
                tmp, new Scope.WholeCluster(),
                new ArchiveSpec(true, 6, "t"), true, 4);
        List<String> argv = MongodumpCommandBuilder.build("mongodump", o);
        assertTrue(argv.contains("--gzip"));
        assertTrue(argv.contains("--oplog"));
        assertTrue(argv.stream().anyMatch(a -> a.startsWith("--uri=")));
        assertTrue(argv.stream().anyMatch(a -> a.startsWith("--out=")));
        assertFalse(argv.stream().anyMatch(a -> a.startsWith("--db=")));
        assertFalse(argv.stream().anyMatch(a -> a.startsWith("--collection=")));
    }

    @Test
    void single_db_scope_emits_db_flag() {
        MongodumpOptions o = new MongodumpOptions(
                "mongodb://h/", tmp,
                new Scope.Databases(List.of("reports")),
                ArchiveSpec.defaults(), false, 4);
        List<String> argv = MongodumpCommandBuilder.build("mongodump", o);
        assertTrue(argv.contains("--db=reports"));
        assertFalse(argv.contains("--oplog"));
    }

    @Test
    void single_namespace_scope_emits_db_and_coll_flags() {
        MongodumpOptions o = new MongodumpOptions(
                "mongodb://h/", tmp,
                new Scope.Namespaces(List.of("shop.orders")),
                ArchiveSpec.defaults(), true, 4);
        List<String> argv = MongodumpCommandBuilder.build("mongodump", o);
        assertTrue(argv.contains("--db=shop"));
        assertTrue(argv.contains("--collection=orders"));
    }

    @Test
    void redactUri_replaces_uri_arg_only() {
        List<String> argv = List.of("mongodump", "--uri=mongodb://u:p@h/",
                "--out=/tmp/out", "--gzip");
        List<String> redacted = MongodumpCommandBuilder.redactUri(argv);
        assertEquals("--uri=<redacted>", redacted.get(1));
        assertEquals("--out=/tmp/out", redacted.get(2));
        assertEquals("--gzip", redacted.get(3));
    }

    /* ============================= progress ============================= */

    @Test
    void progress_bar_line_parses_ns_and_counts() {
        Optional<ProgressLine> parsed = ProgressLine.parse(
                "2026-04-21T10:15:03.456+0000   [####....................]   shop.orders    12345/987654   (1.3%)");
        assertTrue(parsed.isPresent());
        assertEquals("shop.orders", parsed.get().namespace());
        assertEquals(12345L, parsed.get().docsProcessed());
        assertEquals(987654L, parsed.get().totalDocs());
        assertFalse(parsed.get().starting());
        assertFalse(parsed.get().done());
    }

    @Test
    void writing_header_parses_as_starting() {
        Optional<ProgressLine> parsed = ProgressLine.parse(
                "2026-04-21T10:15:03.123+0000   writing shop.orders to dump/shop/orders.bson.gz");
        assertTrue(parsed.isPresent());
        assertEquals("shop.orders", parsed.get().namespace());
        assertTrue(parsed.get().starting());
        assertFalse(parsed.get().done());
    }

    @Test
    void done_dumping_line_marks_completion() {
        Optional<ProgressLine> parsed = ProgressLine.parse(
                "2026-04-21T10:15:04.789+0000   done dumping shop.orders (987654 documents)");
        assertTrue(parsed.isPresent());
        assertEquals(987654L, parsed.get().docsProcessed());
        assertTrue(parsed.get().done());
    }

    @Test
    void unrecognised_lines_return_empty() {
        assertTrue(ProgressLine.parse("random log noise").isEmpty());
        assertTrue(ProgressLine.parse("").isEmpty());
        assertTrue(ProgressLine.parse(null).isEmpty());
    }

    /* ================================ RunLog ============================== */

    @Test
    void runLog_drops_oldest_lines_when_capacity_exceeded() {
        RunLog log = new RunLog(3);
        log.append("one");
        log.append("two");
        log.append("three");
        log.append("four");
        List<String> snap = log.snapshot();
        assertEquals(3, snap.size());
        assertEquals("two", snap.get(0));
        assertEquals("four", snap.get(2));
    }

    @Test
    void runLog_tail_returns_last_n_joined() {
        RunLog log = new RunLog();
        log.append("a"); log.append("b"); log.append("c");
        assertEquals("b\nc", log.tail(2));
        assertEquals("a\nb\nc", log.tail(99));
        assertEquals("", log.tail(0));
    }
}
