package com.kubrik.mex.k8s.preflight;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-G — One check's outcome.
 *
 * <p>Three result levels:</p>
 * <ul>
 *   <li>{@link Status#PASS} — no blocker.</li>
 *   <li>{@link Status#WARN} — recoverable concern; the user must
 *       acknowledge per finding before Apply unlocks (spec §2.7).</li>
 *   <li>{@link Status#FAIL} — hard block; Apply stays disabled.</li>
 * </ul>
 *
 * <p>{@code skipped} is set when the check's scope didn't apply
 * (e.g. cert-manager check on a Dev profile with TLS off). Skipped
 * checks don't contribute to the summary aggregate.</p>
 */
public record PreflightResult(
        String checkId,
        Status status,
        Optional<String> message,
        Optional<String> hint,
        boolean skipped) {

    public enum Status { PASS, WARN, FAIL }

    public PreflightResult {
        Objects.requireNonNull(checkId, "checkId");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(hint, "hint");
    }

    public static PreflightResult pass(String id) {
        return new PreflightResult(id, Status.PASS, Optional.empty(), Optional.empty(), false);
    }

    public static PreflightResult warn(String id, String message, String hint) {
        return new PreflightResult(id, Status.WARN,
                Optional.of(message), Optional.ofNullable(hint), false);
    }

    public static PreflightResult fail(String id, String message, String hint) {
        return new PreflightResult(id, Status.FAIL,
                Optional.of(message), Optional.ofNullable(hint), false);
    }

    public static PreflightResult skipped(String id, String why) {
        return new PreflightResult(id, Status.PASS,
                Optional.of(why), Optional.empty(), true);
    }
}
