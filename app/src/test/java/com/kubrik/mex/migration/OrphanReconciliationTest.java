package com.kubrik.mex.migration;

import com.kubrik.mex.migration.events.JobStatus;
import com.kubrik.mex.migration.profile.ProfileCodec;
import com.kubrik.mex.migration.spec.ConflictMode;
import com.kubrik.mex.migration.spec.ErrorPolicy;
import com.kubrik.mex.migration.spec.ExecutionMode;
import com.kubrik.mex.migration.spec.MigrationKind;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.migration.spec.PerfSpec;
import com.kubrik.mex.migration.spec.ScopeSpec;
import com.kubrik.mex.migration.spec.SourceSpec;
import com.kubrik.mex.migration.spec.TargetSpec;
import com.kubrik.mex.migration.spec.VerifySpec;
import com.kubrik.mex.migration.store.MigrationJobDao;
import com.kubrik.mex.migration.resume.ResumeManager;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** BUG-1 regression: rows left in a non-terminal state by a crashed prior JVM must be marked
 *  {@code FAILED} when the service starts up. */
class OrphanReconciliationTest {

    @TempDir Path dataDir;
    private Database db;
    private MigrationJobDao dao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        dao = new MigrationJobDao(db, new ProfileCodec());
    }

    @AfterEach
    void tearDown() throws Exception { if (db != null) db.close(); }

    @Test
    void reconciles_rows_owned_by_other_pids() {
        JobId orphan = JobId.generate();
        JobId mine = JobId.generate();
        dao.insert(runningRecord(orphan));
        dao.insert(runningRecord(mine));

        long currentPid = ProcessHandle.current().pid();
        dao.stampOwnership(orphan, 999_999L, java.time.Instant.now()); // some other JVM
        dao.stampOwnership(mine, currentPid, java.time.Instant.now());

        int reconciled = dao.reconcileOrphans(currentPid, "Process terminated unexpectedly");
        assertEquals(1, reconciled, "only the foreign-PID row should be reconciled");

        assertEquals(JobStatus.FAILED, dao.get(orphan).orElseThrow().status());
        assertEquals("Process terminated unexpectedly",
                dao.get(orphan).orElseThrow().errorMessage());
        assertEquals(JobStatus.RUNNING, dao.get(mine).orElseThrow().status(),
                "rows owned by the current PID must not be touched");
    }

    @Test
    void reconciles_rows_missing_owner_pid_entirely() {
        JobId legacy = JobId.generate();
        dao.insert(runningRecord(legacy));
        // No stampOwnership call — simulates a row created by a pre-v1.2.0 JVM.

        int reconciled = dao.reconcileOrphans(ProcessHandle.current().pid(),
                "Process terminated unexpectedly");
        assertEquals(1, reconciled);
        assertEquals(JobStatus.FAILED, dao.get(legacy).orElseThrow().status());
    }

    @Test
    void reconciles_rows_with_stale_heartbeat_even_if_owner_pid_matches() {
        // PID reuse after a reboot: a row was stamped by a crashed JVM whose OS PID is the same
        // as the new JVM's PID. Today reconcileOrphans only checks PID equality, so the row
        // survives as RUNNING forever. Fix: also reconcile rows whose last_heartbeat_at is older
        // than a staleness threshold (spec §4.5 — STALE = 60 s).
        JobId reused = JobId.generate();
        dao.insert(runningRecord(reused));
        long currentPid = ProcessHandle.current().pid();
        java.time.Instant ancient = java.time.Instant.now().minus(java.time.Duration.ofMinutes(10));
        dao.stampOwnership(reused, currentPid, ancient);

        int reconciled = dao.reconcileOrphans(currentPid, "Process terminated unexpectedly");
        assertEquals(1, reconciled,
                "a stale heartbeat must reconcile the row even when the PID matches the current JVM");
        assertEquals(JobStatus.FAILED, dao.get(reused).orElseThrow().status());
    }

    @Test
    void leaves_terminal_rows_alone() {
        JobId done = JobId.generate();
        dao.insert(runningRecord(done));
        dao.markEnded(done, JobStatus.COMPLETED, null, 100L, 1024L);

        int reconciled = dao.reconcileOrphans(ProcessHandle.current().pid(),
                "Process terminated unexpectedly");
        assertEquals(0, reconciled);
        assertEquals(JobStatus.COMPLETED, dao.get(done).orElseThrow().status());
    }

    private MigrationJobRecord runningRecord(JobId id) {
        Path jobDir = dataDir.resolve("jobs").resolve(id.value());
        MigrationSpec spec = new MigrationSpec(
                1, MigrationKind.DATA_TRANSFER, "orphan-test",
                new SourceSpec("src-id", "primary"),
                new TargetSpec("tgt-id", null),
                new ScopeSpec.Databases(List.of("app"),
                        new com.kubrik.mex.migration.spec.ScopeFlags(false, false),
                        List.of("**"), List.of(), List.of()),
                null,
                new MigrationSpec.Options(
                        ExecutionMode.RUN,
                        new MigrationSpec.Conflict(ConflictMode.ABORT, Map.of()),
                        Map.of(),
                        PerfSpec.defaults(),
                        VerifySpec.defaults(),
                        ErrorPolicy.defaults(), false, null, List.of()));
        ProfileCodec codec = new ProfileCodec();
        ResumeManager resume = new ResumeManager(jobDir);
        MigrationJobRecord r = com.kubrik.mex.migration.engine.JobRunner.initialRecord(
                id, spec, codec.specHash(spec), jobDir, resume);
        // Flip to RUNNING to simulate a live row.
        return new MigrationJobRecord(
                r.id(), r.kind(), r.sourceConnectionId(), r.targetConnectionId(),
                r.spec(), r.specHash(), JobStatus.RUNNING, r.executionMode(),
                java.time.Instant.now(), null,
                r.docsCopied(), r.bytesCopied(), r.errors(),
                r.errorMessage(), r.resumePath(), r.artifactDir(),
                r.createdAt(), r.updatedAt(),
                r.ownerPid(), r.heartbeatAt(),
                r.sourceConnectionName(), r.targetConnectionName(),
                r.docsProcessed(), r.activeMillis());
    }
}
