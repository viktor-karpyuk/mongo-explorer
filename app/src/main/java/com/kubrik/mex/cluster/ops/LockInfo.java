package com.kubrik.mex.cluster.ops;

import java.util.List;

/**
 * v2.4 LOCK-1..4 — parsed view of {@code lockInfo} command output (3.6+ only).
 * Each entry collapses the raw per-lock detail into the single per-resource
 * summary the UI needs: holders, waiters, the longest hold observed in the
 * sample, and the top op ids so users can jump into the currentOp table.
 */
public record LockInfo(boolean supported, List<Entry> entries, List<TopHolder> topHolders) {

    public static LockInfo unsupported() {
        return new LockInfo(false, List.of(), List.of());
    }

    public int holderCount() { return entries.stream().mapToInt(Entry::holders).sum(); }
    public int waiterCount() { return entries.stream().mapToInt(Entry::waiters).sum(); }

    /** A single lock resource — namespace for coll locks, {@code global} / {@code local}
     *  for database-level ones, {@code ParallelBatchWriterMode} for PBWM, etc. */
    public record Entry(String resource, int holders, int waiters, long maxHoldMs, String mode) {}

    /** Top lock holder, row-linkable to the currentOp table via opid. */
    public record TopHolder(long opid, String resource, long heldMs) {}
}
