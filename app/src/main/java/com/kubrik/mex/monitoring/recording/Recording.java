package com.kubrik.mex.monitoring.recording;

import java.util.List;
import java.util.Objects;

/**
 * Recording metadata row, persisted in {@code recordings}. See technical-spec §3.1.
 *
 * <p>{@code endedAtMs} and {@code stopReason} are {@code null} while the recording
 * is {@code ACTIVE} or {@code PAUSED}; both are non-null once the row reaches
 * {@code STOPPED} (invariant enforced by {@code RecordingService}).
 *
 * <p>{@code schemaVersion} starts at 1 and is bumped only when metric-ID semantics
 * change across releases — the field lives here so older bundles round-trip faithfully.
 */
public record Recording(
        String id,
        String connectionId,
        String name,
        String note,
        List<String> tags,
        RecordingState state,
        StopReason stopReason,
        long startedAtMs,
        Long endedAtMs,
        long pausedMillis,
        Long maxDurationMs,
        Long maxSizeBytes,
        boolean captureProfilerSamples,
        long createdAtMs,
        int schemaVersion
) {
    public Recording {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(connectionId, "connectionId");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(state, "state");
        tags = tags == null ? List.of() : List.copyOf(tags);
        if (state == RecordingState.STOPPED) {
            if (endedAtMs == null) throw new IllegalArgumentException("STOPPED requires endedAtMs");
            if (stopReason == null) throw new IllegalArgumentException("STOPPED requires stopReason");
        } else {
            if (endedAtMs != null) throw new IllegalArgumentException(state + " forbids endedAtMs");
            if (stopReason != null) throw new IllegalArgumentException(state + " forbids stopReason");
        }
        if (pausedMillis < 0) throw new IllegalArgumentException("pausedMillis must be ≥ 0");
        if (schemaVersion < 1) throw new IllegalArgumentException("schemaVersion must be ≥ 1");
    }

    public Recording withName(String newName) {
        return new Recording(id, connectionId, newName, note, tags, state, stopReason,
                startedAtMs, endedAtMs, pausedMillis, maxDurationMs, maxSizeBytes,
                captureProfilerSamples, createdAtMs, schemaVersion);
    }

    public Recording withNote(String newNote) {
        return new Recording(id, connectionId, name, newNote, tags, state, stopReason,
                startedAtMs, endedAtMs, pausedMillis, maxDurationMs, maxSizeBytes,
                captureProfilerSamples, createdAtMs, schemaVersion);
    }

    public Recording withTags(List<String> newTags) {
        return new Recording(id, connectionId, name, note, newTags, state, stopReason,
                startedAtMs, endedAtMs, pausedMillis, maxDurationMs, maxSizeBytes,
                captureProfilerSamples, createdAtMs, schemaVersion);
    }

    public Recording withId(String newId) {
        return new Recording(newId, connectionId, name, note, tags, state, stopReason,
                startedAtMs, endedAtMs, pausedMillis, maxDurationMs, maxSizeBytes,
                captureProfilerSamples, createdAtMs, schemaVersion);
    }

    /** Build the next state in the lifecycle — used by {@code RecordingService}. */
    public Recording withTransition(RecordingState newState, StopReason newStopReason,
                                    Long newEndedAtMs, long newPausedMillis) {
        return new Recording(id, connectionId, name, note, tags, newState, newStopReason,
                startedAtMs, newEndedAtMs, newPausedMillis, maxDurationMs, maxSizeBytes,
                captureProfilerSamples, createdAtMs, schemaVersion);
    }

    /** Effective capture duration — wall time minus paused intervals. */
    public long effectiveDurationMs(long nowMs) {
        long endOrNow = endedAtMs != null ? endedAtMs : nowMs;
        return Math.max(0, endOrNow - startedAtMs - pausedMillis);
    }
}
