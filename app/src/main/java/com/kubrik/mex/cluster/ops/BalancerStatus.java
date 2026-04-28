package com.kubrik.mex.cluster.ops;

/**
 * v2.4 SHARD-5..9 — parsed view of {@code balancerStatus} + balancer window
 * metadata. {@code mode} preserves the server's tri-state ("full", "autoSplitOnly",
 * "off") so the UI can render a richer pill than a boolean would allow.
 */
public record BalancerStatus(
        boolean supported,
        String mode,
        boolean inRound,
        long rounds,
        String windowStart,
        String windowStop,
        int activeMigrations,
        long chunksMovedLast24h,
        String errorMessage
) {
    public static BalancerStatus unsupported(String why) {
        return new BalancerStatus(false, "unknown", false, 0L, null, null, 0, 0L, why);
    }

    public boolean enabled() { return "full".equalsIgnoreCase(mode) || "autoSplitOnly".equalsIgnoreCase(mode); }
    public boolean hasWindow() { return windowStart != null && windowStop != null; }
}
