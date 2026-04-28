package com.kubrik.mex.migration;

import com.kubrik.mex.migration.events.JobStatus;
import com.kubrik.mex.migration.spec.MigrationKind;

import java.time.Instant;
import java.util.Set;

/** Filter for {@link MigrationService#list}. All fields are optional; nulls mean "no constraint".
 *  <p>{@code offset} only has effect when {@code limit > 0} — an unlimited query ignores offset. */
public record JobHistoryQuery(
        Set<JobStatus> statuses,
        Set<MigrationKind> kinds,
        String connectionId,       // matches source_conn_id OR target_conn_id
        Instant startedAfter,
        Instant startedBefore,
        int limit,                 // 0 = unlimited
        int offset                 // rows to skip; requires limit > 0
) {
    public JobHistoryQuery {
        if (offset < 0) throw new IllegalArgumentException("offset must be >= 0");
        if (limit < 0) throw new IllegalArgumentException("limit must be >= 0");
    }

    /** Back-compat 6-arg constructor — callers before pagination landed did not pass an offset. */
    public JobHistoryQuery(Set<JobStatus> statuses,
                           Set<MigrationKind> kinds,
                           String connectionId,
                           Instant startedAfter,
                           Instant startedBefore,
                           int limit) {
        this(statuses, kinds, connectionId, startedAfter, startedBefore, limit, 0);
    }

    public static JobHistoryQuery all() {
        return new JobHistoryQuery(null, null, null, null, null, 0, 0);
    }

    /** A single page, 0-indexed, with filters reset. */
    public static JobHistoryQuery page(int pageIndex, int pageSize) {
        if (pageIndex < 0 || pageSize <= 0) {
            throw new IllegalArgumentException("pageIndex must be >= 0 and pageSize > 0");
        }
        return new JobHistoryQuery(null, null, null, null, null, pageSize, pageIndex * pageSize);
    }

    /** Copy this query with a different page window. Filters are preserved. */
    public JobHistoryQuery withPage(int pageIndex, int pageSize) {
        if (pageIndex < 0 || pageSize <= 0) {
            throw new IllegalArgumentException("pageIndex must be >= 0 and pageSize > 0");
        }
        return new JobHistoryQuery(statuses, kinds, connectionId, startedAfter, startedBefore,
                pageSize, pageIndex * pageSize);
    }
}
