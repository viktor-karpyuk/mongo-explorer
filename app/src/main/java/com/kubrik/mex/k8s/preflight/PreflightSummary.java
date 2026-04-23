package com.kubrik.mex.k8s.preflight;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * v2.8.1 Q2.8.1-G — Aggregate pre-flight outcome the UI panel renders.
 *
 * <p>{@link #hasAnyFail()} disables the Apply button.
 * {@link #warnsToAck()} drives the per-warning checkbox list.
 * {@link #passing()} / {@link #skipped()} populate the "green" +
 * "n/a" sections of the panel.</p>
 */
public record PreflightSummary(List<PreflightResult> results) {

    public PreflightSummary {
        Objects.requireNonNull(results, "results");
        results = Collections.unmodifiableList(new ArrayList<>(results));
    }

    public boolean hasAnyFail() {
        return results.stream().anyMatch(r -> r.status() == PreflightResult.Status.FAIL);
    }

    public List<PreflightResult> warnsToAck() {
        return results.stream()
                .filter(r -> !r.skipped() && r.status() == PreflightResult.Status.WARN)
                .toList();
    }

    public List<PreflightResult> passing() {
        return results.stream()
                .filter(r -> !r.skipped() && r.status() == PreflightResult.Status.PASS)
                .toList();
    }

    public List<PreflightResult> failing() {
        return results.stream()
                .filter(r -> r.status() == PreflightResult.Status.FAIL)
                .toList();
    }

    public List<PreflightResult> skipped() {
        return results.stream().filter(PreflightResult::skipped).toList();
    }

    public int total() { return results.size(); }
}
