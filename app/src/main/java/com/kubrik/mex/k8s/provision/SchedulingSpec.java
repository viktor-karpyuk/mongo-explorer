package com.kubrik.mex.k8s.provision;

/**
 * v2.8.1 Q2.8.1-D1 — PDB + TopologySpread controls.
 *
 * <p>Prod locks {@code pdbEnabled} + {@code topologySpread} to
 * {@code true}; Dev/Test defaults them off so single-node kind
 * clusters apply without fighting anti-affinity.</p>
 */
public record SchedulingSpec(boolean pdbEnabled, boolean topologySpread) {

    public static SchedulingSpec devDefaults() { return new SchedulingSpec(false, false); }
    public static SchedulingSpec prodDefaults() { return new SchedulingSpec(true, true); }
}
