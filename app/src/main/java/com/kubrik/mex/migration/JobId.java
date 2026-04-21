package com.kubrik.mex.migration;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/** Opaque job identifier. Format: {@code j-<utc-timestamp>-<short-uuid>}.
 *  <p>
 *  Embedding the timestamp makes filesystem sort order match chronological order for the
 *  per-job artefact directories under {@code jobs/}. */
public record JobId(String value) {

    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

    public static JobId generate() {
        String ts = FMT.format(Instant.now());
        String rand = UUID.randomUUID().toString().substring(0, 8);
        return new JobId("j-" + ts + "-" + rand);
    }

    public static JobId of(String v) {
        if (v == null || v.isBlank()) throw new IllegalArgumentException("JobId cannot be blank");
        return new JobId(v);
    }

    @Override
    public String toString() { return value; }
}
