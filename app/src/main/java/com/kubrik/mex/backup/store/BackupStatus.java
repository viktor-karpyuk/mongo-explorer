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
    CANCELLED
}
