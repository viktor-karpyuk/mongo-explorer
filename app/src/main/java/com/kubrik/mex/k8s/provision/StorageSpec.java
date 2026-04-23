package com.kubrik.mex.k8s.provision;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-D1 — Storage class + PVC sizes per member kind.
 *
 * <p>Member kinds for v2.8.1 Alpha: data, configServer (sharded
 * only), mongos (sharded only, empty-dir — size ignored). Split
 * so Prod can enforce non-empty on {@code dataSizeGib} without
 * caring about mongos's ephemeral storage.</p>
 *
 * <p>{@code storageClass} empty → use the namespace's default
 * storage class; pre-flight verifies one exists before Apply.</p>
 */
public record StorageSpec(
        Optional<String> storageClass,
        int dataSizeGib,
        int configServerSizeGib) {

    public StorageSpec {
        Objects.requireNonNull(storageClass, "storageClass");
        if (dataSizeGib < 0) throw new IllegalArgumentException(
                "dataSizeGib must be non-negative: " + dataSizeGib);
        if (configServerSizeGib < 0) throw new IllegalArgumentException(
                "configServerSizeGib must be non-negative: " + configServerSizeGib);
    }

    public static StorageSpec devDefaults() {
        return new StorageSpec(Optional.empty(), 10, 2);
    }

    public static StorageSpec prodDefaults() {
        return new StorageSpec(Optional.empty(), 100, 10);
    }
}
