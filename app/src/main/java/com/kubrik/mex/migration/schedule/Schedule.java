package com.kubrik.mex.migration.schedule;

import java.time.Instant;

/** A row in {@code migration_schedules} — a named profile plus the cron expression that
 *  fires it. {@code nextRunAt} is recomputed after every successful dispatch so the
 *  background scheduler can cheaply list due schedules without walking the cron on every
 *  tick. {@code lastRunAt} is nullable — unset until the first fire. */
public record Schedule(
        String id,
        String profileId,
        String cron,
        boolean enabled,
        Instant lastRunAt,
        Instant nextRunAt,
        Instant createdAt
) {}
