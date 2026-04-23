package com.kubrik.mex.labs.model;

/**
 * v2.8.4 Q2.8.4-D — Lab lifecycle state machine.
 *
 * <p>Transitions are guarded by {@code LabLifecycleService}; rejected
 * calls return a structured error so the UI can render the exact
 * reason (wrong current state, missing Docker, etc.).</p>
 *
 * <pre>
 *   apply()             stop()
 * ─► CREATING ─► RUNNING ◀───── STOPPED
 *        │         │   start()    ▲
 *        ▼         └──────────────┤
 *     FAILED                      │
 *        │                        │
 *        └─ destroy() (any) ──────┼─► DESTROYED
 * </pre>
 */
public enum LabStatus {
    /** Apply kicked off; compose up in flight OR health-watch polling. */
    CREATING,
    /** Containers healthy, auto-connection wired. */
    RUNNING,
    /** {@code docker compose stop} was invoked; volumes preserved. */
    STOPPED,
    /** Apply failed at some step; cleanup ran. */
    FAILED,
    /** {@code docker compose down -v} ran; row is a tombstone. */
    DESTROYED;

    public boolean isTerminal() {
        return this == DESTROYED;
    }

    public boolean isLive() {
        return this == CREATING || this == RUNNING || this == STOPPED;
    }
}
