package com.kubrik.mex.backup;

import com.kubrik.mex.backup.runner.BackupScheduler;
import com.kubrik.mex.backup.spec.ArchiveSpec;
import com.kubrik.mex.backup.spec.BackupPolicy;
import com.kubrik.mex.backup.spec.RetentionSpec;
import com.kubrik.mex.backup.spec.Scope;
import com.kubrik.mex.backup.store.BackupCatalogDao;
import com.kubrik.mex.backup.store.BackupCatalogRow;
import com.kubrik.mex.backup.store.BackupPolicyDao;
import com.kubrik.mex.backup.store.BackupStatus;
import com.kubrik.mex.backup.store.SinkDao;
import com.kubrik.mex.backup.store.SinkRecord;
import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.5 BKP-SCHED-1..5 — scheduler tick, orphan reconcile, and missed-run
 * backfill driven by a deterministic {@link Clock}.
 */
class SchedulerTickTest {

    @TempDir Path dataDir;

    private Database db;
    private BackupPolicyDao policies;
    private BackupCatalogDao catalog;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        policies = new BackupPolicyDao(db);
        catalog = new BackupCatalogDao(db);
        // v2.6.1 — backup_policies.sink_id now carries a FK to
        // storage_sinks(id). Seed sink #1 so samplePolicy (which uses
        // sinkId=1) can insert.
        new SinkDao(db, new Crypto()).insert(new SinkRecord(-1,
                "LOCAL_FS", "sink-1", "/tmp/sink-1", null, null, 1L, 1L));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void tick_fires_dispatcher_when_cron_matches_current_minute() {
        BackupPolicy saved = policies.insert(samplePolicy("cx-a", "daily", "0 3 * * *"));

        // Freeze clock at 2026-04-21 03:00:00 UTC — matches "0 3 * * *".
        Instant at = Instant.parse("2026-04-21T03:00:00Z");
        List<Long> fired = new CopyOnWriteArrayList<>();
        BackupScheduler s = new BackupScheduler(policies, catalog,
                p -> fired.add(p.id()),
                Clock.fixed(at, ZoneId.of("UTC")));
        s.tick();
        assertEquals(List.of(saved.id()), fired, "cron match fires dispatcher");

        // Second tick in the same minute is a no-op (dedup via lastFired map).
        s.tick();
        assertEquals(1, fired.size(), "duplicate tick in the same minute is dedup'd");
    }

    @Test
    void tick_skips_policies_whose_cron_does_not_match() {
        policies.insert(samplePolicy("cx-a", "daily", "0 3 * * *"));
        Instant at = Instant.parse("2026-04-21T04:00:00Z");  // one hour past
        List<Long> fired = new CopyOnWriteArrayList<>();
        BackupScheduler s = new BackupScheduler(policies, catalog,
                p -> fired.add(p.id()),
                Clock.fixed(at, ZoneId.of("UTC")));
        s.tick();
        assertTrue(fired.isEmpty());
    }

    @Test
    void concurrent_dispatch_is_skipped_until_markDone() {
        BackupPolicy saved = policies.insert(samplePolicy("cx-a", "minutely", "* * * * *"));
        Instant at = Instant.parse("2026-04-21T03:05:00Z");
        List<Long> fired = new CopyOnWriteArrayList<>();
        BackupScheduler s = new BackupScheduler(policies, catalog,
                p -> fired.add(p.id()),
                Clock.fixed(at, ZoneId.of("UTC")));
        s.tick();
        // At a different minute — would normally fire again, but policy is still
        // marked running. Advance clock by constructing a fresh scheduler +
        // transferring the running flag via not calling markDone.
        BackupScheduler s2 = new BackupScheduler(policies, catalog,
                p -> fired.add(p.id()),
                Clock.fixed(at.plusSeconds(60), ZoneId.of("UTC")));
        // New scheduler instance has a fresh running set; to test the
        // real-world behaviour we re-use s at the next minute after marking it
        // as running externally — simulate by calling tick twice on the same
        // scheduler with different clocks via a mutable clock.
        MutableClock m = new MutableClock(Instant.parse("2026-04-21T03:05:00Z"));
        BackupScheduler s3 = new BackupScheduler(policies, catalog,
                p -> fired.add(p.id()), m);
        fired.clear();
        s3.tick();
        m.set(Instant.parse("2026-04-21T03:06:00Z"));
        s3.tick();  // policy still running — skipped
        assertEquals(1, fired.size(), "second tick skipped because run is still in flight");
        s3.markDone(saved.id());
        m.set(Instant.parse("2026-04-21T03:07:00Z"));
        s3.tick();
        assertEquals(2, fired.size(), "post-markDone tick dispatches again");
    }

    @Test
    void reconcileOrphans_flips_RUNNING_rows_to_FAILED_on_start() {
        BackupPolicy saved = policies.insert(samplePolicy("cx-a", "daily", "0 3 * * *"));
        BackupCatalogRow running = catalog.insert(new BackupCatalogRow(-1,
                saved.id(), "cx-a", 1_000L, null, BackupStatus.RUNNING,
                saved.sinkId(), "<running>", null, null, null, null, null,
                null, null, null));

        BackupScheduler s = new BackupScheduler(policies, catalog,
                p -> {}, Clock.fixed(Instant.parse("2026-04-21T10:00:00Z"),
                ZoneId.of("UTC")));
        s.reconcileOrphans();

        BackupCatalogRow after = catalog.byId(running.id()).orElseThrow();
        assertEquals(BackupStatus.FAILED, after.status());
        assertTrue(after.notes() != null && after.notes().contains("crashed"));
    }

    @Test
    void backfillMissed_writes_synthetic_rows_for_past_firings() {
        BackupPolicy saved = policies.insert(samplePolicy("cx-a", "hourly", "0 * * * *"));
        // "now" = 12:05 UTC on 2026-04-21 — hourly cron should have fired at
        // 11:00, 12:00. No catalog rows exist, so both are missed.
        Instant now = Instant.parse("2026-04-21T12:05:00Z");
        BackupScheduler s = new BackupScheduler(policies, catalog,
                p -> {}, Clock.fixed(now, ZoneId.of("UTC")));
        s.backfillMissed();

        List<BackupCatalogRow> rows = catalog.listForPolicy(saved.id());
        long missedCount = rows.stream()
                .filter(r -> r.status() == BackupStatus.MISSED).count();
        // 24h window backwards from now: 12:05 → yesterday 12:05. That's 24
        // complete hourly firings (yesterday 13:00 through today 12:00).
        assertTrue(missedCount >= 23 && missedCount <= 25,
                "expected ~24 missed firings, got " + missedCount);
        // A second backfill is idempotent — no duplicate rows.
        s.backfillMissed();
        assertEquals(missedCount, catalog.listForPolicy(saved.id()).stream()
                .filter(r -> r.status() == BackupStatus.MISSED).count());
    }

    /* ============================ fixtures ============================ */

    private static BackupPolicy samplePolicy(String cx, String name, String cron) {
        return new BackupPolicy(-1, cx, name, true, cron,
                new Scope.WholeCluster(), ArchiveSpec.defaults(),
                RetentionSpec.defaults(), 1L, true, 1L, 1L);
    }

    private static final class MutableClock extends Clock {
        private volatile Instant now;
        MutableClock(Instant initial) { this.now = initial; }
        void set(Instant next) { this.now = next; }
        @Override public ZoneId getZone() { return ZoneId.of("UTC"); }
        @Override public Clock withZone(ZoneId z) { return this; }
        @Override public Instant instant() { return now; }
    }
}
