package com.kubrik.mex.migration.events;

/** Terminal + non-terminal job states. See docs/mvp-functional-spec.md §9 for the state machine
 *  and label/colour conventions. */
public enum JobStatus {
    PENDING,
    RUNNING,
    PAUSING,
    PAUSED,
    CANCELLING,
    CANCELLED,
    COMPLETED,
    COMPLETED_WITH_WARNINGS,
    FAILED;

    public boolean isTerminal() {
        return this == CANCELLED || this == COMPLETED
                || this == COMPLETED_WITH_WARNINGS || this == FAILED;
    }

    public boolean isActive() {
        return this == RUNNING || this == PAUSING || this == PAUSED || this == CANCELLING;
    }
}
