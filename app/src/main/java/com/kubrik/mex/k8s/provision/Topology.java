package com.kubrik.mex.k8s.provision;

/**
 * v2.8.1 Q2.8.1-D1 — Cluster topology (member layout).
 *
 * <p>Availability is operator + profile dependent — see
 * {@link TopologyPicker#availableTopologies}. v2.8.1 Alpha blessed
 * set (milestone §7.8): standalone + RS3 for Dev; RS3 + RS5 for
 * Prod; SHARDED only on Prod + PSMDB. 7-node RS / multi-shard
 * custom layouts are explicitly out of scope pending a later
 * blessed-matrix expansion.</p>
 */
public enum Topology {
    STANDALONE,
    RS3,
    RS5,
    SHARDED;

    /** Number of mongod replicas implied by the topology (per shard for SHARDED). */
    public int replicasPerReplset() {
        return switch (this) {
            case STANDALONE -> 1;
            case RS3, SHARDED -> 3;   // SHARDED = 3-shard × 3-replica starter (open question §9.7)
            case RS5 -> 5;
        };
    }

    /** Shard count for the starter preset (milestone §9.7). */
    public int shardCount() {
        return this == SHARDED ? 3 : 1;
    }
}
