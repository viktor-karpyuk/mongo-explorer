package com.kubrik.mex.monitoring.recording;

/**
 * Why a recording stopped. Clean shutdown and uncaught crash both converge on
 * {@link #INTERRUPTED} — see technical-spec §4.5 and §5.2.
 */
public enum StopReason {
    MANUAL,
    AUTO_DURATION,
    AUTO_SIZE,
    CONNECTION_LOST,
    INTERRUPTED
}
