package com.kubrik.mex.cluster.audit;

/**
 * v2.4 AUD-1 — terminal outcome of an audited destructive action. {@link #PENDING}
 * is reserved for long-running commands whose result lands on the next topology
 * tick (see {@code MoveChunk} watchdog in §8.4 risk register).
 */
public enum Outcome {
    OK,
    FAIL,
    CANCELLED,
    PENDING
}
