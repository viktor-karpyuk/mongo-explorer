package com.kubrik.mex.maint.model;

import java.util.Objects;

/**
 * v2.7 Q2.7-F — A parameter tuning recommendation emitted by the
 * recommender. {@link #severity} drives whether the UI chip renders
 * red (ACT), amber (CONSIDER), or grey (INFO).
 */
public record ParamProposal(
        String param,
        String currentValue,
        String proposedValue,
        Severity severity,
        String rationale
) {
    public enum Severity { INFO, CONSIDER, ACT }

    public ParamProposal {
        Objects.requireNonNull(param, "param");
        Objects.requireNonNull(currentValue, "currentValue");
        Objects.requireNonNull(proposedValue, "proposedValue");
        Objects.requireNonNull(severity, "severity");
        Objects.requireNonNull(rationale, "rationale");
    }

    /** Whether this proposal actually moves a value. If current ==
     *  proposed, the recommender emitted an INFO row (the param is
     *  already tuned); the UI shows it greyed-out. */
    public boolean isActionable() {
        return !currentValue.equals(proposedValue);
    }
}
