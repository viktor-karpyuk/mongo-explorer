package com.kubrik.mex.k8s.compute.managedpool;

/**
 * v2.8.4 Q2.8.4-A — Which cloud managed-pool adapter to route a
 * {@link com.kubrik.mex.k8s.compute.ComputeStrategy.ManagedPool}
 * request through.
 *
 * <p>GA ships EKS (§1.6). GKE + AKS are point-release follow-ons;
 * each gets its own adapter but the type here stays stable so the
 * JSON codec + DAO never churn when a new cloud lands.</p>
 */
public enum CloudProvider {
    AWS, GCP, AZURE;

    public static CloudProvider fromWire(String raw) {
        if (raw == null) throw new IllegalArgumentException("provider required");
        return switch (raw.toUpperCase()) {
            case "AWS", "EKS" -> AWS;
            case "GCP", "GKE" -> GCP;
            case "AZURE", "AKS" -> AZURE;
            default -> throw new IllegalArgumentException("unknown cloud provider: " + raw);
        };
    }

    public String wireValue() {
        return name();
    }
}
