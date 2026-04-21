package com.kubrik.mex.backup.runner;

/**
 * v2.5 BKP-RUN-7/8 — result record of one {@link MongodumpRunner#run} call.
 *
 * <p>{@code exitCode} follows mongodump's convention: 0 = success, non-zero
 * = failure. {@code stderrTail} carries up to 100 lines from {@link RunLog}
 * so a failed run leaves a useful breadcrumb without keeping the whole log
 * in memory. {@code killed} distinguishes an external cancellation (SIGTERM
 * / SIGKILL triggered by the caller) from a mongodump-self-terminated
 * failure.</p>
 */
public record DumpOutcome(int exitCode, boolean killed, long durationMs, String stderrTail) {
    public boolean ok() { return exitCode == 0 && !killed; }
}
