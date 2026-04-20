package com.kubrik.mex.migration.schedule;

import com.kubrik.mex.migration.profile.ProfileCodec;
import com.kubrik.mex.migration.spec.ConflictMode;
import com.kubrik.mex.migration.spec.ErrorPolicy;
import com.kubrik.mex.migration.spec.ExecutionMode;
import com.kubrik.mex.migration.spec.MigrationKind;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.migration.spec.Namespace;
import com.kubrik.mex.migration.spec.PerfSpec;
import com.kubrik.mex.migration.spec.ScopeFlags;
import com.kubrik.mex.migration.spec.ScopeSpec;
import com.kubrik.mex.migration.spec.SourceSpec;
import com.kubrik.mex.migration.spec.TargetSpec;
import com.kubrik.mex.migration.spec.VerifySpec;
import com.kubrik.mex.migration.store.ProfileStore;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Scheduler semantics — dispatch on the next tick, advance {@code next_run_at}, degrade
 *  gracefully when the dispatch target throws, and disable schedules whose cron expression
 *  can't be parsed. Uses the real SQLite store + a mutable fake {@link Clock}. */
class MigrationSchedulerTest {

    private static final ZoneId UTC = ZoneId.of("UTC");

    @TempDir Path dataDir;
    private Database db;
    private ProfileStore profiles;
    private ScheduleStore schedules;
    private String profileId;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        profiles = new ProfileStore(db, new ProfileCodec());
        schedules = new ScheduleStore(db);
        profileId = profiles.save("scheduled", sampleSpec()).id();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
        System.clearProperty("user.home");
    }

    @Test
    void tick_fires_due_schedules_and_advances_next_run() {
        // Fire every minute on the minute.
        Instant t0 = ZonedDateTime.of(2026, 4, 18, 10, 0, 0, 0, UTC).toInstant();
        schedules.create(profileId, "* * * * *", true, t0);

        MutableClock clock = new MutableClock(t0);
        CopyOnWriteArrayList<MigrationSpec> fired = new CopyOnWriteArrayList<>();
        MigrationScheduler scheduler = new MigrationScheduler(schedules, profiles, fired::add,
                clock, UTC, Duration.ofSeconds(30));

        scheduler.tick();
        assertEquals(1, fired.size(), "due schedule fires once on tick");

        Schedule row = schedules.list().get(0);
        assertEquals(t0, row.lastRunAt(), "last_run_at recorded");
        assertEquals(t0.plusSeconds(60), row.nextRunAt(), "next_run_at advanced to next minute");

        // Same tick at the same clock: nothing new.
        scheduler.tick();
        assertEquals(1, fired.size(), "no double-fire within the same minute");

        // Advance the clock past the next boundary.
        clock.instant = t0.plusSeconds(61);
        scheduler.tick();
        assertEquals(2, fired.size(), "fires again after the clock crosses next_run_at");
    }

    @Test
    void disabled_schedules_are_skipped() {
        Instant t0 = ZonedDateTime.of(2026, 4, 18, 10, 0, 0, 0, UTC).toInstant();
        schedules.create(profileId, "* * * * *", /*enabled=*/ false, t0);

        MutableClock clock = new MutableClock(t0);
        AtomicInteger fires = new AtomicInteger();
        MigrationScheduler scheduler = new MigrationScheduler(schedules, profiles,
                spec -> fires.incrementAndGet(), clock, UTC, Duration.ofSeconds(30));

        scheduler.tick();
        assertEquals(0, fires.get(), "disabled schedules never fire");
    }

    @Test
    void dispatcher_failure_advances_next_run_so_we_dont_spin() {
        Instant t0 = ZonedDateTime.of(2026, 4, 18, 10, 0, 0, 0, UTC).toInstant();
        String id = schedules.create(profileId, "* * * * *", true, t0).id();

        MutableClock clock = new MutableClock(t0);
        AtomicInteger attempts = new AtomicInteger();
        MigrationScheduler scheduler = new MigrationScheduler(schedules, profiles,
                spec -> { attempts.incrementAndGet(); throw new IllegalStateException("busy"); },
                clock, UTC, Duration.ofSeconds(30));

        scheduler.tick();
        assertEquals(1, attempts.get(), "dispatcher invoked once");

        Schedule row = schedules.get(id).orElseThrow();
        assertNull(row.lastRunAt(), "last_run_at not set when the dispatcher fails");
        assertEquals(t0.plusSeconds(60), row.nextRunAt(),
                "next_run_at still advances so we don't retry every tick");
    }

    @Test
    void unparseable_cron_disables_the_schedule() {
        Instant t0 = ZonedDateTime.of(2026, 4, 18, 10, 0, 0, 0, UTC).toInstant();
        String id = schedules.create(profileId, "not a cron", true, t0).id();

        MutableClock clock = new MutableClock(t0);
        // Dispatcher refuses so computeNextRun is called in advanceWithoutFiring, which
        // trips the parse error and disables the row.
        MigrationScheduler scheduler = new MigrationScheduler(schedules, profiles,
                spec -> { throw new IllegalStateException("busy"); },
                clock, UTC, Duration.ofSeconds(30));

        scheduler.tick();
        Schedule row = schedules.get(id).orElseThrow();
        assertFalse(row.enabled(), "unparseable cron disables the schedule");
    }

    // Note: the missing-profile branch in MigrationScheduler is defensive; the
    // `migration_schedules.profile_id` FK with ON DELETE CASCADE makes that state unreachable
    // via the normal flow (deleting a profile cascades the schedule). Test skipped.

    // --- helpers -----------------------------------------------------------------

    private static MigrationSpec sampleSpec() {
        return new MigrationSpec(
                1, MigrationKind.DATA_TRANSFER, "scheduled",
                new SourceSpec("src", "primary"),
                new TargetSpec("tgt", null),
                new ScopeSpec.Collections(
                        List.of(Namespace.parse("app.users")),
                        new ScopeFlags(false, false),
                        List.of("**"), List.of(), List.of()),
                null,
                new MigrationSpec.Options(
                        ExecutionMode.RUN,
                        new MigrationSpec.Conflict(ConflictMode.APPEND, Map.of()),
                        Map.of(),
                        PerfSpec.defaults(),
                        VerifySpec.defaults(),
                        ErrorPolicy.defaults(), false, null, List.of()));
    }

    /** Clock that returns whatever {@link #instant} is set to. Good enough for the firing
     *  logic under test — we just need to move time forward deterministically. */
    private static final class MutableClock extends Clock {
        volatile Instant instant;
        MutableClock(Instant instant) { this.instant = instant; }
        @Override public ZoneId getZone() { return UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return instant; }
    }

}
