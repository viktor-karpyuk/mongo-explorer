package com.kubrik.mex.migration.resume;

import java.time.Instant;
import java.util.List;

/** On-disk resume state (docs/mvp-technical-spec.md §5.3). Serialised to {@code resume.json}
 *  and reloaded on crash/cancel recovery. BSON values are represented as extended-JSON
 *  strings so the file is human-readable. */
public record ResumeFile(
        int schema,
        String jobId,
        String specHash,
        List<String> completed,          // source namespaces fully copied
        InProgress inProgress,           // null if no collection is mid-flight
        Instant savedAt
) {

    public record InProgress(
            String collection,           // source namespace
            String targetNamespace,
            String lastIdJson,           // extended-JSON of last written _id; null if none yet
            long docsWritten,
            List<PartitionCheckpoint> partitions
    ) {}

    public record PartitionCheckpoint(
            String minJson,              // inclusive lower bound (extended-JSON)
            String maxJson,              // exclusive upper bound (extended-JSON)
            String lastIdJson,           // last written _id in this partition (extended-JSON)
            long docsWritten
    ) {}
}
