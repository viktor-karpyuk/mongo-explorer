package com.kubrik.mex.migration;

import com.kubrik.mex.migration.schedule.MigrationSchedule;
import com.kubrik.mex.migration.schedule.ScheduleDao;
import com.kubrik.mex.migration.schedule.SchedulerService;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SchedulerServiceTest {

    @TempDir Path tmp;
    private Database db;
    private ScheduleDao dao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", tmp.toString());
        db = new Database();
        dao = new ScheduleDao(db);
        // FK on migration_schedules.profile_id requires a real profile row.
        seedProfile("profile-1");
        seedProfile("profile-2");
    }

    private void seedProfile(String id) throws Exception {
        try (var ps = db.connection().prepareStatement(
                "INSERT INTO migration_profiles (id, name, kind, spec_json, created_at, updated_at) " +
                        "VALUES (?, ?, ?, ?, ?, ?)")) {
            ps.setString(1, id);
            ps.setString(2, id);
            ps.setString(3, "DATA_TRANSFER");
            ps.setString(4, "{}");
            ps.setLong(5, 0L);
            ps.setLong(6, 0L);
            ps.executeUpdate();
        }
    }

    @AfterEach
    void tearDown() throws Exception { db.close(); }

    @Test
    void persistedScheduleRoundTrips() throws Exception {
        String id = UUID.randomUUID().toString();
        MigrationSchedule s = new MigrationSchedule(
                id, "profile-1", "@every 15m", true, null, null, 1_000L);
        dao.upsert(s);

        MigrationSchedule loaded = dao.find(id).orElseThrow();
        assertEquals("profile-1", loaded.profileId());
        assertEquals("@every 15m", loaded.expression());
        assertTrue(loaded.enabled());
        assertNull(loaded.lastRunAtMs());
    }

    @Test
    void neverRunScheduleIsDueImmediately() throws Exception {
        dao.upsert(new MigrationSchedule(
                "s1", "profile-1", "@every 1h", true, null, null, 0L));
        List<MigrationSchedule> due = dao.dueAt(1_000L);
        assertEquals(1, due.size());
    }

    @Test
    void tickRunsDueSchedulesAndAdvancesNextRun() throws Exception {
        long nowMs = 1_700_000_000_000L;
        dao.upsert(new MigrationSchedule(
                "s1", "profile-1", "@every 1m", true, null, null, nowMs - 1_000));
        dao.upsert(new MigrationSchedule(
                "s2", "profile-2", "@every 1h", false, null, null, nowMs - 1_000));

        CopyOnWriteArrayList<String> invoked = new CopyOnWriteArrayList<>();
        Clock fixed = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.UTC);
        SchedulerService svc = new SchedulerService(dao, s -> invoked.add(s.id()), fixed);

        int fired = svc.tickOnce();
        assertEquals(1, fired, "only the enabled schedule fires");
        assertEquals(List.of("s1"), invoked);

        MigrationSchedule s1 = dao.find("s1").orElseThrow();
        assertEquals(Long.valueOf(nowMs), s1.lastRunAtMs());
        assertEquals(Long.valueOf(nowMs + 60_000L), s1.nextRunAtMs());
    }

    @Test
    void runnerExceptionsStillAdvanceNextRun() throws Exception {
        long nowMs = 1_700_000_000_000L;
        dao.upsert(new MigrationSchedule(
                "bad", "profile-1", "@every 1m", true, null, null, 0L));
        Clock fixed = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.UTC);
        SchedulerService svc = new SchedulerService(dao,
                s -> { throw new RuntimeException("boom"); },
                fixed);
        svc.tickOnce();

        MigrationSchedule loaded = dao.find("bad").orElseThrow();
        assertEquals(Long.valueOf(nowMs + 60_000L), loaded.nextRunAtMs(),
                "a thrown runner must not trap the schedule in a hot loop");
    }

    @Test
    void invalidExpressionPushesNextRunFarIntoTheFuture() throws Exception {
        long nowMs = 1_700_000_000_000L;
        dao.upsert(new MigrationSchedule(
                "bad", "profile-1", "0 * * * *" /* unsupported v2.0 */, true, null, null, 0L));
        Clock fixed = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.UTC);
        SchedulerService svc = new SchedulerService(dao, s -> {}, fixed);
        svc.tickOnce();

        MigrationSchedule loaded = dao.find("bad").orElseThrow();
        assertTrue(loaded.nextRunAtMs() > nowMs + 3_000L * 24 * 3600 * 1000L,
                "invalid expression must be pushed far-future so we don't re-tick");
    }
}
