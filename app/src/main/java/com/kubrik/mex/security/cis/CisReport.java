package com.kubrik.mex.security.cis;

import java.util.List;
import java.util.Objects;

/** v2.6 Q2.6-H1 — scored CIS scan report. Counts are pre-computed so the
 *  header card renders without a second pass over findings. */
public record CisReport(
        String connectionId,
        long ranAtMs,
        String benchmarkVersion,
        List<CisFinding> findings,
        int pass,
        int fail,
        int notApplicable,
        int suppressed
) {
    public CisReport {
        Objects.requireNonNull(connectionId, "connectionId");
        Objects.requireNonNull(benchmarkVersion, "benchmarkVersion");
        findings = findings == null ? List.of() : List.copyOf(findings);
    }

    public int total() { return findings.size(); }

    public boolean clean() { return fail == 0; }
}
