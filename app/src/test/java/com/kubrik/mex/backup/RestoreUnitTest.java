package com.kubrik.mex.backup;

import com.kubrik.mex.backup.runner.MongorestoreCommandBuilder;
import com.kubrik.mex.backup.runner.MongorestoreOptions;
import com.kubrik.mex.backup.runner.RestoreProgressLine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.5 Q2.5-E — unit coverage for the restore-side primitives (argv builder
 * + progress parser). Live subprocess round-trip is deferred to a §5 IT
 * that shells out to mongorestore.
 */
class RestoreUnitTest {

    @TempDir Path tmp;

    /* ========================== command builder ========================== */

    @Test
    void dry_run_restore_builds_expected_argv() {
        MongorestoreOptions o = new MongorestoreOptions(
                "mongodb://user:pass@h/",
                tmp, Map.of(), true, true, true, false, 4);
        List<String> argv = MongorestoreCommandBuilder.build("mongorestore", o);
        assertTrue(argv.contains("--dryRun"));
        assertTrue(argv.contains("--drop"));
        assertTrue(argv.contains("--gzip"));
        assertTrue(argv.stream().anyMatch(a -> a.startsWith("--dir=")));
        assertFalse(argv.contains("--oplogReplay"));
    }

    @Test
    void namespace_rename_emits_nsFrom_and_nsTo_pairs() {
        MongorestoreOptions o = new MongorestoreOptions(
                "mongodb://h/", tmp,
                Map.of("reports.*", "reports_rehearse.*"),
                false, true, false, false, 4);
        List<String> argv = MongorestoreCommandBuilder.build("mongorestore", o);
        assertTrue(argv.contains("--nsFrom=reports.*"));
        assertTrue(argv.contains("--nsTo=reports_rehearse.*"));
        assertFalse(argv.contains("--drop"));
        assertFalse(argv.contains("--gzip"));
    }

    @Test
    void oplogLimit_is_emitted_when_set_with_oplog_replay_on() {
        // v2.6 Q2.6-L5 — PITR handoff: the picker passes the planner's
        // epoch-seconds cut-off; the builder must emit --oplogLimit
        // <secs>:0 only when oplogReplay is on.
        MongorestoreOptions withPitr = new MongorestoreOptions(
                "mongodb://h/", tmp, Map.of(),
                false, false, false, /*oplogReplay=*/true, 4,
                /*oplogLimitTsSecs=*/1_700_000_500L);
        List<String> argv = MongorestoreCommandBuilder.build("mongorestore", withPitr);
        assertTrue(argv.contains("--oplogReplay"));
        assertTrue(argv.contains("--oplogLimit=1700000500:0"),
                "--oplogLimit must land in argv with :0 ordinal suffix");
    }

    @Test
    void oplogLimit_is_not_emitted_when_oplog_replay_is_off() {
        // Defensive: without oplog replay the limit flag would be invalid
        // mongorestore input. Builder must elide it.
        MongorestoreOptions noReplay = new MongorestoreOptions(
                "mongodb://h/", tmp, Map.of(),
                false, false, false, /*oplogReplay=*/false, 4,
                /*oplogLimitTsSecs=*/1_700_000_500L);
        List<String> argv = MongorestoreCommandBuilder.build("mongorestore", noReplay);
        assertTrue(argv.stream().noneMatch(a -> a.startsWith("--oplogLimit")));
    }

    @Test
    void legacy_constructor_without_oplogLimit_still_works() {
        // Back-compat: v2.5 call-sites that don't set the new field
        // continue to compile + produce argv without --oplogLimit.
        MongorestoreOptions legacy = new MongorestoreOptions(
                "mongodb://h/", tmp, Map.of(),
                false, false, false, true, 4);
        List<String> argv = MongorestoreCommandBuilder.build("mongorestore", legacy);
        assertTrue(argv.contains("--oplogReplay"));
        assertTrue(argv.stream().noneMatch(a -> a.startsWith("--oplogLimit")));
    }

    @Test
    void redactUri_reuses_mongodump_policy() {
        List<String> argv = List.of("mongorestore", "--uri=mongodb://u:p@h/", "--dir=/x");
        List<String> redacted = MongorestoreCommandBuilder.redactUri(argv);
        assertEquals("--uri=<redacted>", redacted.get(1));
    }

    /* ============================= progress ============================= */

    @Test
    void progress_line_parses_ns_and_counts() {
        Optional<RestoreProgressLine> parsed = RestoreProgressLine.parse(
                "2026-04-21T10:15:04.456+0000  [##....]  reports.daily  12345/98765  (12.5%)");
        assertTrue(parsed.isPresent());
        assertEquals("reports.daily", parsed.get().namespace());
        assertEquals(12345L, parsed.get().docsProcessed());
        assertEquals(98765L, parsed.get().totalDocs());
        assertFalse(parsed.get().starting());
        assertFalse(parsed.get().done());
    }

    @Test
    void restoring_header_parses_as_starting() {
        Optional<RestoreProgressLine> parsed = RestoreProgressLine.parse(
                "2026-04-21T10:15:03.123+0000  restoring reports.daily from dump/reports/daily.bson.gz");
        assertTrue(parsed.isPresent());
        assertTrue(parsed.get().starting());
    }

    @Test
    void finished_line_captures_failures() {
        Optional<RestoreProgressLine> parsed = RestoreProgressLine.parse(
                "2026-04-21T10:15:05.789+0000  finished restoring reports.daily (98765 documents, 3 failures)");
        assertTrue(parsed.isPresent());
        assertTrue(parsed.get().done());
        assertEquals(3L, parsed.get().failures());
    }

    @Test
    void unrecognised_restore_lines_return_empty() {
        assertTrue(RestoreProgressLine.parse("foo bar").isEmpty());
        assertTrue(RestoreProgressLine.parse(null).isEmpty());
    }
}
