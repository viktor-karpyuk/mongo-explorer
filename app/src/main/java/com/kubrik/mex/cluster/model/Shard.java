package com.kubrik.mex.cluster.model;

import java.util.List;
import java.util.Map;

/** v2.4 TOPO-12 — per-shard entry inside a sharded {@link TopologySnapshot}. */
public record Shard(
        String id,
        String rsHost,
        boolean draining,
        Map<String, String> tags,
        List<Member> members
) {
    public Shard {
        if (id == null) throw new IllegalArgumentException("id");
        if (rsHost == null) rsHost = "";
        if (tags == null) tags = Map.of();
        if (members == null) members = List.of();
    }
}
