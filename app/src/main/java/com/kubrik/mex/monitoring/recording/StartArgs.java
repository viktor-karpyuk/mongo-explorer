package com.kubrik.mex.monitoring.recording;

import java.util.List;
import java.util.Objects;

/**
 * Immutable start-time arguments validated by
 * {@code RecordingService.start(connectionId, args)}.
 *
 * <p>{@code maxDurationMs} / {@code maxSizeBytes} of {@code null} mean "no limit".
 * See requirements §2.3 and technical-spec §3.3.
 */
public record StartArgs(
        String name,
        String note,
        List<String> tags,
        Long maxDurationMs,
        Long maxSizeBytes,
        boolean captureProfilerSamples
) {
    public StartArgs {
        Objects.requireNonNull(name, "name");
        if (name.isBlank() || name.length() > 120) {
            throw new IllegalArgumentException("name must be 1–120 chars");
        }
        if (note != null && note.length() > 2_000) {
            throw new IllegalArgumentException("note must be ≤ 2000 chars");
        }
        tags = tags == null ? List.of() : List.copyOf(tags);
        if (tags.size() > 16) {
            throw new IllegalArgumentException("at most 16 tags per recording");
        }
        for (String t : tags) {
            if (t == null || t.isBlank() || t.length() > 32) {
                throw new IllegalArgumentException("tag must be 1–32 chars");
            }
        }
        if (maxDurationMs != null && maxDurationMs <= 0) {
            throw new IllegalArgumentException("maxDurationMs must be positive");
        }
        if (maxSizeBytes != null && maxSizeBytes <= 0) {
            throw new IllegalArgumentException("maxSizeBytes must be positive");
        }
    }
}
