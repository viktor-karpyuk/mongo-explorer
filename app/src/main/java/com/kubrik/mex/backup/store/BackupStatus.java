package com.kubrik.mex.backup.store;

/**
 * v2.5 BKP-RUN-1..8 — terminal status of a backup run. {@link #RUNNING} is the
 * transient state between the START audit row and the END audit row; catalog
 * rows are persisted in this state so a hard crash / JVM kill is recoverable
 * (Q2.5-C reconciles RUNNING rows older than the heartbeat window to FAILED).
 */
public enum BackupStatus {
    RUNNING,
    OK,
    FAILED,
    CANCELLED,
    /** v2.5 BKP-SCHED-2 — a scheduled tick's due-time passed while the app
     *  was closed. Written as a synthetic catalog row with
     *  {@code started_at = finished_at = dueTimeMs} so the history surface
     *  can show the gap without a runner invocation. */
    MISSED
}
