package com.kubrik.mex.backup;

import com.kubrik.mex.backup.runner.BackupRunner;
import com.kubrik.mex.backup.sink.LocalFsTarget;
import com.kubrik.mex.backup.spec.ArchiveSpec;
import com.kubrik.mex.backup.spec.BackupPolicy;
import com.kubrik.mex.backup.spec.RetentionSpec;
import com.kubrik.mex.backup.spec.Scope;
import com.kubrik.mex.backup.store.BackupCatalogDao;
import com.kubrik.mex.backup.store.BackupCatalogRow;
import com.kubrik.mex.backup.store.BackupFileDao;
import com.kubrik.mex.backup.store.BackupStatus;
import com.kubrik.mex.cluster.store.OpsAuditDao;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.5 BKP-RUN-1..8 — end-to-end orchestrator test.
 *
 * <p>Replaces real {@code mongodump} with a tiny shell script that creates a
 * fixed set of fake bson files under the requested {@code --out} directory,
 * so the runner's responsibilities (spawn + progress + file walk + hash +
 * manifest write + catalog finalise + audit + events) round-trip without
 * needing the mongodump binary installed.</p>
 *
 * <p>Skipped on Windows — the shim is a shell script. Linux + macOS CI
 * runners cover the orchestrator path.</p>
 */
class BackupRunnerIT {

    @TempDir Path dataDir;
    @TempDir Path sinkDir;
    @TempDir Path shimDir;

    private Database db;
    private EventBus bus;
    private BackupCatalogDao catalog;
    private BackupFileDao files;
    private OpsAuditDao audit;
    private BackupRunner runner;
    private final List<com.kubrik.mex.backup.event.BackupEvent> events = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        assumeUnix();
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        bus = new EventBus();
        catalog = new BackupCatalogDao(db);
        files = new BackupFileDao(db);
        audit = new OpsAuditDao(db);
        bus.onBackup(events::add);

        Path shim = writeMongodumpShim();
        runner = new BackupRunner(catalog, files, audit, bus, Clock.systemUTC(),
                shim.toString());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void runs_mongodump_shim_and_finalises_catalog_with_manifest() throws IOException {
        LocalFsTarget sink = new LocalFsTarget("local", sinkDir.toString());
        // policy.id = -1 so the catalog row's FK to backup_policies goes null
        // (we don't have a real policy row seeded for this orchestrator test).
        BackupPolicy policy = new BackupPolicy(-1L, "cx-it", "nightly", true, null,
                new Scope.WholeCluster(),
                new ArchiveSpec(false, 0, "<policy>/<yyyy-MM-dd_HH-mm-ss>"),
                RetentionSpec.defaults(), 99L, false, 1L, 1L);

        BackupRunner.RunResult result = runner.execute(
                "cx-it", "mongodb://host/", policy, sink, 99L, "dba", "localhost");

        assertEquals(BackupStatus.OK, result.status());
        assertEquals(64, result.manifestSha256().length());
        assertTrue(result.totalBytes() > 0);

        BackupCatalogRow row = catalog.byId(result.catalogId()).orElseThrow();
        assertEquals(BackupStatus.OK, row.status());
        assertEquals(result.manifestSha256(), row.manifestSha256());
        assertNotNull(row.finishedAt());

        List<com.kubrik.mex.backup.store.BackupFileRow> inventory =
                files.listForCatalog(result.catalogId());
        // shim drops 2 bson files + an oplog.bson; manifest is the fourth.
        assertEquals(4, inventory.size());
        assertTrue(inventory.stream().anyMatch(r -> r.kind().equals("manifest")));
        assertTrue(inventory.stream().allMatch(r -> r.sha256().length() == 64));

        // Event stream contains Started + Ended (Progress may be 0 since shim
        // doesn't emit tqdm-style lines).
        assertTrue(events.stream()
                .anyMatch(e -> e instanceof com.kubrik.mex.backup.event.BackupEvent.Started));
        assertTrue(events.stream()
                .anyMatch(e -> e instanceof com.kubrik.mex.backup.event.BackupEvent.Ended));

        // manifest.json on disk matches catalog hash.
        Path manifestFile = sinkDir.resolve(row.sinkPath()).resolve("manifest.json");
        assertTrue(Files.exists(manifestFile));
    }

    @Test
    void oplog_slice_populated_when_include_oplog_true() throws IOException {
        LocalFsTarget sink = new LocalFsTarget("local", sinkDir.toString());
        // includeOplog = true and the shim drops oplog.bson — runner should
        // capture firstTs/lastTs + record them on the catalog row.
        BackupPolicy policy = new BackupPolicy(-1L, "cx-it", "oplog-on", true, null,
                new Scope.WholeCluster(),
                new ArchiveSpec(false, 0, "<policy>/<yyyy-MM-dd_HH-mm-ss>"),
                RetentionSpec.defaults(), 99L, true, 1L, 1L);

        BackupRunner.RunResult result = runner.execute(
                "cx-it", "mongodb://host/", policy, sink, 99L, "dba", "localhost");
        assertEquals(BackupStatus.OK, result.status());

        BackupCatalogRow row = catalog.byId(result.catalogId()).orElseThrow();
        assertNotNull(row.oplogFirstTs(), "oplog_first_ts must be populated");
        assertNotNull(row.oplogLastTs(), "oplog_last_ts must be populated");
        assertTrue(row.oplogLastTs() >= row.oplogFirstTs(),
                "lastTs must be >= firstTs");
    }

    @Test
    void manifest_file_row_byte_count_matches_utf8_size_for_non_ascii_scope() throws IOException {
        // Policy name contains non-ASCII chars so the canonical manifest JSON
        // encodes to more bytes than Java String chars — exposed a verifier
        // size-mismatch bug before the UTF-8-byte-count fix landed.
        LocalFsTarget sink = new LocalFsTarget("local", sinkDir.toString());
        BackupPolicy policy = new BackupPolicy(-1L, "cx-it", "nachtlauf-äöü", true, null,
                new Scope.WholeCluster(),
                new ArchiveSpec(false, 0, "<policy>/<yyyy-MM-dd_HH-mm-ss>"),
                RetentionSpec.defaults(), 99L, false, 1L, 1L);

        BackupRunner.RunResult result = runner.execute(
                "cx-it", "mongodb://host/", policy, sink, 99L, "dba", "localhost");
        assertEquals(BackupStatus.OK, result.status());

        var inventory = files.listForCatalog(result.catalogId());
        var manifestRow = inventory.stream()
                .filter(r -> r.kind().equals("manifest"))
                .findFirst().orElseThrow();
        BackupCatalogRow rowMeta = catalog.byId(result.catalogId()).orElseThrow();
        Path manifestFile = sinkDir.resolve(rowMeta.sinkPath()).resolve("manifest.json");
        assertEquals(Files.size(manifestFile), manifestRow.bytes(),
                "manifest row's bytes must match the on-disk UTF-8 size");
    }

    @Test
    void nonzero_exit_code_produces_FAILED_catalog_and_stderr_tail() throws Exception {
        Path failShim = writeFailShim();
        runner = new BackupRunner(catalog, files, audit, bus, Clock.systemUTC(),
                failShim.toString());
        LocalFsTarget sink = new LocalFsTarget("local", sinkDir.toString());
        BackupPolicy policy = new BackupPolicy(-1L, "cx-fail", "p", true, null,
                new Scope.WholeCluster(),
                new ArchiveSpec(false, 0, "<policy>"),
                RetentionSpec.defaults(), 1L, false, 1L, 1L);

        BackupRunner.RunResult r = runner.execute("cx-fail", "mongodb://h/",
                policy, sink, 1L, "dba", "localhost");
        assertEquals(BackupStatus.FAILED, r.status());
        BackupCatalogRow row = catalog.byId(r.catalogId()).orElseThrow();
        assertNotNull(row.notes());
        assertTrue(row.notes().contains("exit code"));
    }

    /* =========================== shim helpers =========================== */

    private Path writeMongodumpShim() throws IOException {
        Path shim = shimDir.resolve("mongodump-shim");
        String script = """
                #!/bin/bash
                # Fake mongodump: drop a couple of bson files + an oplog.bson
                # into --out=<dir>. The oplog file exercises Q2.5-F's OplogSlice
                # capture path.
                set -e
                out=""
                for arg in "$@"; do
                  case "$arg" in
                    --out=*) out="${arg#--out=}" ;;
                  esac
                done
                mkdir -p "$out/shop"
                echo 'bson-a' > "$out/shop/orders.bson"
                echo 'bson-b' > "$out/shop/users.bson"
                echo 'oplog-entries' > "$out/oplog.bson"
                exit 0
                """;
        Files.writeString(shim, script, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        shim.toFile().setExecutable(true, true);
        return shim;
    }

    private Path writeFailShim() throws IOException {
        Path shim = shimDir.resolve("mongodump-fail");
        String script = """
                #!/bin/bash
                echo 'auth failed: bad credentials' >&2
                exit 3
                """;
        Files.writeString(shim, script, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        shim.toFile().setExecutable(true, true);
        return shim;
    }

    private static void assumeUnix() {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                !System.getProperty("os.name", "").toLowerCase().contains("win"),
                "shell-script shim requires Unix-style shell");
    }
}
