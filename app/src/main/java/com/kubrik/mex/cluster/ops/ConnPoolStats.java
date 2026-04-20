package com.kubrik.mex.cluster.ops;

import java.util.List;

/**
 * v2.4 POOL-1..5 — parsed {@code connPoolStats} output. A single record holds
 * the per-host rows plus the {@code totalInUse / totalAvailable / totalCreated}
 * roll-up, kept separate so the UI footer doesn't have to re-sum.
 */
public record ConnPoolStats(List<Row> rows, int totalInUse, int totalAvailable, int totalCreated) {

    public static ConnPoolStats empty() {
        return new ConnPoolStats(List.of(), 0, 0, 0);
    }

    /** A single pool — one per remote host the driver talks to. */
    public record Row(
            String host,
            int poolSize,
            int inUse,
            int available,
            int created,
            int refreshing,
            int waitQueueSize,
            long timeouts,
            Long lastRefreshedMs
    ) {}
}
