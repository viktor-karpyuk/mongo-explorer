package com.kubrik.mex.cluster.model;

/** v2.4 TOPO-13 — single mongos entry inside a sharded {@link TopologySnapshot}. */
public record Mongos(
        String host,
        String version,
        Long uptimeSecs,
        Long advisoryStartupDelaySecs
) {
    public Mongos {
        if (host == null) throw new IllegalArgumentException("host");
    }
}
