package com.kubrik.mex.migration.schedule;

/**
 * One row of {@code migration_schedules} (UX-7). {@code expression} is either an
 * interval-shorthand ({@code @every 15m}, {@code @every 2h}, {@code @every 1d}) or —
 * once full cron is implemented — a 5-field cron expression. {@code nextRunAtMs}
 * is absolute wall-clock time; {@code null} until the scheduler computes the first
 * fire time.
 */
public record MigrationSchedule(
        String id,
        String profileId,
        String expression,
        boolean enabled,
        Long lastRunAtMs,
        Long nextRunAtMs,
        long createdAtMs
) {}
