package com.kubrik.mex.migration.schedule;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for {@link CronExpression}. All assertions run in UTC to keep day/week math
 *  deterministic regardless of the test host's zone. */
class CronExpressionTest {

    private static final ZoneId UTC = ZoneId.of("UTC");

    @Test
    void five_field_literal_every_day_at_2am() {
        CronExpression cron = CronExpression.parse("0 2 * * *");
        // Thursday 2026-04-17 18:00 UTC → next fire should be 2026-04-18 02:00 UTC.
        Instant from = ZonedDateTime.of(2026, 4, 17, 18, 0, 0, 0, UTC).toInstant();
        Instant expected = ZonedDateTime.of(2026, 4, 18, 2, 0, 0, 0, UTC).toInstant();
        assertEquals(expected, cron.nextFireAfter(from, UTC));
    }

    @Test
    void star_wildcard_fires_every_minute() {
        CronExpression cron = CronExpression.parse("* * * * *");
        Instant from = ZonedDateTime.of(2026, 4, 18, 12, 34, 56, 0, UTC).toInstant();
        Instant expected = ZonedDateTime.of(2026, 4, 18, 12, 35, 0, 0, UTC).toInstant();
        assertEquals(expected, cron.nextFireAfter(from, UTC));
    }

    @Test
    void step_every_fifteen_minutes() {
        CronExpression cron = CronExpression.parse("*/15 * * * *");
        Instant from = ZonedDateTime.of(2026, 4, 18, 12, 7, 0, 0, UTC).toInstant();
        Instant expected = ZonedDateTime.of(2026, 4, 18, 12, 15, 0, 0, UTC).toInstant();
        assertEquals(expected, cron.nextFireAfter(from, UTC));
    }

    @Test
    void step_every_fifteen_minutes_advances_into_next_hour() {
        CronExpression cron = CronExpression.parse("*/15 * * * *");
        Instant from = ZonedDateTime.of(2026, 4, 18, 12, 47, 0, 0, UTC).toInstant();
        // After :45 the next bucket is :00 of the following hour.
        Instant expected = ZonedDateTime.of(2026, 4, 18, 13, 0, 0, 0, UTC).toInstant();
        assertEquals(expected, cron.nextFireAfter(from, UTC));
    }

    @Test
    void range_and_list_in_one_field() {
        // Business hours only, weekdays — fire on the hour.
        CronExpression cron = CronExpression.parse("0 9-17 * * 1-5");
        Instant friNoon  = ZonedDateTime.of(2026, 4, 17, 12, 30, 0, 0, UTC).toInstant();
        Instant expected = ZonedDateTime.of(2026, 4, 17, 13, 0, 0, 0, UTC).toInstant();
        assertEquals(expected, cron.nextFireAfter(friNoon, UTC));

        // Late Friday: next fire is Monday 09:00.
        Instant friLate = ZonedDateTime.of(2026, 4, 17, 18, 1, 0, 0, UTC).toInstant();
        Instant mon09    = ZonedDateTime.of(2026, 4, 20, 9, 0, 0, 0, UTC).toInstant();
        assertEquals(mon09, cron.nextFireAfter(friLate, UTC));
    }

    @Test
    void day_of_week_sunday_is_zero() {
        // "2am every Sunday"
        CronExpression cron = CronExpression.parse("0 2 * * 0");
        Instant thu = ZonedDateTime.of(2026, 4, 16, 12, 0, 0, 0, UTC).toInstant();
        Instant sun = ZonedDateTime.of(2026, 4, 19, 2, 0, 0, 0, UTC).toInstant();
        assertEquals(sun, cron.nextFireAfter(thu, UTC));
    }

    @Test
    void dom_and_dow_both_restricted_fires_on_either() {
        // Classic Vixie-cron: "1st of the month OR any Monday, at 00:00"
        CronExpression cron = CronExpression.parse("0 0 1 * 1");
        // Wednesday 2026-04-15 12:00: next Monday is 2026-04-20.
        Instant from = ZonedDateTime.of(2026, 4, 15, 12, 0, 0, 0, UTC).toInstant();
        Instant mon  = ZonedDateTime.of(2026, 4, 20, 0, 0, 0, 0, UTC).toInstant();
        assertEquals(mon, cron.nextFireAfter(from, UTC));

        // From late April, the 1st of May (a Friday) comes before the next Monday (May 4).
        Instant late = ZonedDateTime.of(2026, 4, 28, 12, 0, 0, 0, UTC).toInstant();
        Instant may1 = ZonedDateTime.of(2026, 5, 1, 0, 0, 0, 0, UTC).toInstant();
        assertEquals(may1, cron.nextFireAfter(late, UTC));
    }

    @Test
    void invalid_expressions_rejected() {
        assertThrows(IllegalArgumentException.class, () -> CronExpression.parse(null));
        assertThrows(IllegalArgumentException.class, () -> CronExpression.parse(""));
        assertThrows(IllegalArgumentException.class, () -> CronExpression.parse("* * * *"));   // 4 fields
        assertThrows(IllegalArgumentException.class, () -> CronExpression.parse("* * * * * *")); // 6 fields
        assertThrows(IllegalArgumentException.class, () -> CronExpression.parse("60 * * * *")); // minute > 59
        assertThrows(IllegalArgumentException.class, () -> CronExpression.parse("0 24 * * *")); // hour > 23
        assertThrows(IllegalArgumentException.class, () -> CronExpression.parse("0 0 0 * *")); // dom < 1
        assertThrows(IllegalArgumentException.class, () -> CronExpression.parse("0 0 32 * *")); // dom > 31
        assertThrows(IllegalArgumentException.class, () -> CronExpression.parse("0 0 * 13 *")); // month > 12
        assertThrows(IllegalArgumentException.class, () -> CronExpression.parse("0 0 * * 7"));  // dow > 6
        assertThrows(IllegalArgumentException.class, () -> CronExpression.parse("5-1 * * * *")); // inverted range
        assertThrows(IllegalArgumentException.class, () -> CronExpression.parse("*/0 * * * *")); // step 0
    }

    @Test
    void matches_ignores_seconds() {
        CronExpression cron = CronExpression.parse("30 14 * * *");
        Instant onTheMinute = ZonedDateTime.of(2026, 4, 18, 14, 30, 0, 0, UTC).toInstant();
        Instant midMinute   = ZonedDateTime.of(2026, 4, 18, 14, 30, 42, 0, UTC).toInstant();
        assertTrue(cron.matches(onTheMinute, UTC));
        assertTrue(cron.matches(midMinute, UTC));
    }
}
