package com.kubrik.mex.migration.events;

import com.kubrik.mex.migration.JobId;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.migration.verify.VerificationReport;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

/** All events published on {@link com.kubrik.mex.events.EventBus#publishJob} for a running
 *  migration. Sealed so subscribers can switch exhaustively. */
public sealed interface JobEvent {

    JobId jobId();
    Instant timestamp();

    record Started(JobId jobId, Instant timestamp, MigrationSpec spec) implements JobEvent {}

    record Progress(JobId jobId, Instant timestamp, ProgressSnapshot snapshot) implements JobEvent {}

    record LogLine(
            JobId jobId,
            Instant timestamp,
            String level,
            String collection,
            String op,
            String message,
            Map<String, Object> fields
    ) implements JobEvent {}

    record StatusChanged(JobId jobId, Instant timestamp, JobStatus newStatus) implements JobEvent {}

    record Completed(JobId jobId, Instant timestamp, VerificationReport report) implements JobEvent {}

    record Failed(JobId jobId, Instant timestamp, String error, Path resumeFile) implements JobEvent {}

    record Cancelled(JobId jobId, Instant timestamp, Path resumeFile) implements JobEvent {}
}
