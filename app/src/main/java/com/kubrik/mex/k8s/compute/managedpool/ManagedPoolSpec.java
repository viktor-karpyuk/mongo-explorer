package com.kubrik.mex.k8s.compute.managedpool;

import java.util.List;
import java.util.Objects;

/**
 * v2.8.4 Q2.8.4-A/B — Managed node-pool strategy inputs.
 *
 * <p>Mirrors milestone-v2.8.4.md §2.2's wizard shape: a cloud-
 * credential pointer, the pool spec (region, name, instance type,
 * capacity type, counts, arch, zones), and Mongo labels/taints
 * auto-populated at render time. Cloud-specific capacity values
 * (e.g. EKS's {@code ON_DEMAND} / {@code SPOT}) are stored as
 * canonical-cased strings — the adapter translates to the cloud's
 * wire format.</p>
 */
public record ManagedPoolSpec(
        CloudProvider provider,
        long credentialId,
        String region,
        String poolName,
        String instanceType,
        CapacityType capacityType,
        int minNodes,
        int desiredNodes,
        int maxNodes,
        String arch,
        List<String> zones,
        List<String> subnetIds
) {
    public enum CapacityType { ON_DEMAND, SPOT }

    public ManagedPoolSpec {
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(region, "region");
        Objects.requireNonNull(poolName, "poolName");
        Objects.requireNonNull(instanceType, "instanceType");
        Objects.requireNonNull(capacityType, "capacityType");
        if (minNodes < 0 || desiredNodes < minNodes || maxNodes < desiredNodes) {
            throw new IllegalArgumentException(
                    "invalid pool sizing: min=" + minNodes
                    + " desired=" + desiredNodes + " max=" + maxNodes);
        }
        arch = arch == null ? "amd64" : arch;
        zones = zones == null ? List.of() : List.copyOf(zones);
        subnetIds = subnetIds == null ? List.of() : List.copyOf(subnetIds);
    }

    /** Sensible AWS EKS starter — matches milestone §2.2's defaults
     *  (on-demand m6i.large, 3-node pool, amd64). */
    public static ManagedPoolSpec sensibleEksDefaults(long credentialId, String region,
                                                       String deploymentName) {
        return new ManagedPoolSpec(
                CloudProvider.AWS, credentialId, region,
                "mex-" + safeName(deploymentName),
                "m6i.large", CapacityType.ON_DEMAND,
                3, 3, 5, "amd64",
                List.of(), List.of());
    }

    private static String safeName(String raw) {
        return raw.toLowerCase().replaceAll("[^a-z0-9-]", "-").replaceAll("-+", "-");
    }
}
