package com.kubrik.mex.security.drift;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * v2.6 Q2.6-D3 — one row in {@code sec_drift_acks}. Either:
 * <ul>
 *   <li><b>ACK</b> — expected change on a specific baseline diff; hides
 *       the finding for that {@code baselineId} only. A future capture
 *       would re-surface the same path.</li>
 *   <li><b>MUTE</b> — path is deliberately fluctuating; hides the finding
 *       across every future diff until the user removes the mute. Useful
 *       for {@code users.*.authenticationRestrictions.lastSuccessfulAuth}-
 *       style paths the server rewrites each login.</li>
 * </ul>
 */
public record DriftAck(
        long id,
        String connectionId,
        long baselineId,
        String path,
        long ackedAt,
        String ackedBy,
        Mode mode,
        String note
) {
    public enum Mode { ACK, MUTE }

    public DriftAck {
        Objects.requireNonNull(connectionId, "connectionId");
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(mode, "mode");
        if (ackedBy == null) ackedBy = "";
        if (note == null) note = "";
    }

    /** Filter a list of drift findings against the ack / mute rows for the
     *  current baseline. ACKs hide findings whose path matches and whose
     *  ack row was recorded against the <em>same</em> baseline id. MUTEs
     *  hide matching paths regardless of baseline. */
    public static List<DriftFinding> hideAcked(List<DriftFinding> findings,
                                                 long currentBaselineId,
                                                 List<DriftAck> acks) {
        if (findings == null || findings.isEmpty()) return List.of();
        if (acks == null || acks.isEmpty()) return List.copyOf(findings);

        Set<String> mutedPaths = acks.stream()
                .filter(a -> a.mode() == Mode.MUTE)
                .map(DriftAck::path).collect(java.util.stream.Collectors.toSet());
        Set<String> ackedHerePaths = acks.stream()
                .filter(a -> a.mode() == Mode.ACK && a.baselineId() == currentBaselineId)
                .map(DriftAck::path).collect(java.util.stream.Collectors.toSet());

        return findings.stream()
                .filter(f -> !mutedPaths.contains(f.path())
                        && !ackedHerePaths.contains(f.path()))
                .toList();
    }
}
