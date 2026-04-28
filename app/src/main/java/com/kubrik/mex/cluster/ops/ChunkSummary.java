package com.kubrik.mex.cluster.ops;

import java.util.List;
import java.util.Map;

/**
 * v2.4 SHARD-10..13 — per-collection chunk distribution summary. The UI
 * renders one row per namespace with a compact shard → count histogram and a
 * jumbo chunk flag.
 */
public record ChunkSummary(
        String ns,
        long totalChunks,
        long jumboChunks,
        Map<String, Long> perShard
) {
    public ChunkSummary {
        if (perShard == null) perShard = Map.of();
    }

    /** Distinct shard names sorted, for consistent column ordering. */
    public List<String> shardsSorted() {
        return perShard.keySet().stream().sorted().toList();
    }
}
