package com.kubrik.mex.migration;

import com.kubrik.mex.migration.events.JobStatus;
import com.kubrik.mex.migration.spec.ExecutionMode;
import com.kubrik.mex.migration.spec.MigrationKind;
import com.kubrik.mex.migration.spec.MigrationSpec;

import java.nio.file.Path;
import java.time.Instant;

/** Row in {@code migration_jobs} plus the parsed {@link MigrationSpec}. The immutable view
 *  returned from {@link MigrationService#get} and {@link MigrationService#list}.
 *  <p>
 *  {@code ownerPid} and {@code heartbeatAt} are populated by {@code MigrationService} when
 *  the job starts running; they stay non-null while the owning JVM is alive and are cleared
 *  by {@code markEnded}. Startup reconciliation (BUG-1) uses them to detect orphaned rows.
 *  <p>
 *  {@code sourceConnectionName} / {@code targetConnectionName} are stamped at insert time
 *  (UX-11) so the history table still renders readably after a connection is renamed or
 *  deleted. {@code docsProcessed} (OBS-5) counts docs through the read/transform pipeline
 *  — equal to {@code docsCopied} on RUN, populated independently on DRY_RUN. {@code
 *  activeMillis} (OBS-7) is wall-clock time spent RUNNING, excluding paused windows. */
public record MigrationJobRecord(
        JobId id,
        MigrationKind kind,
        String sourceConnectionId,
        String targetConnectionId,
        MigrationSpec spec,
        String specHash,
        JobStatus status,
        ExecutionMode executionMode,
        Instant startedAt,
        Instant endedAt,
        long docsCopied,
        long bytesCopied,
        long errors,
        String errorMessage,
        Path resumePath,
        Path artifactDir,
        Instant createdAt,
        Instant updatedAt,
        Long ownerPid,
        Instant heartbeatAt,
        String sourceConnectionName,
        String targetConnectionName,
        long docsProcessed,
        long activeMillis
) {}
