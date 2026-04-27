package com.kubrik.mex.k8s.compute.managedpool;

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

/**
 * v2.8.4 Q2.8.4-B — Runtime registry of which cloud adapters are
 * linked into the build.
 *
 * <p>GA ships {@link EksAdapterStub} (AWS); GKE + AKS land as point
 * releases and are injected via {@link #register} at Main wiring
 * time. A provider without a registered adapter causes the managed-
 * pool preflight to fail with an actionable hint.</p>
 */
public final class ManagedPoolAdapterRegistry {

    private final Map<CloudProvider, ManagedPoolAdapter> adapters =
            new EnumMap<>(CloudProvider.class);

    public ManagedPoolAdapterRegistry register(ManagedPoolAdapter a) {
        adapters.put(a.provider(), a);
        return this;
    }

    public Optional<ManagedPoolAdapter> lookup(CloudProvider provider) {
        return Optional.ofNullable(adapters.get(provider));
    }

    /** GA default — three stubs covering AWS / GCP / Azure. Production
     *  bootstraps replace each with the SDK-backed implementation as
     *  the per-cloud point releases land. */
    public static ManagedPoolAdapterRegistry defaultRegistry() {
        return new ManagedPoolAdapterRegistry()
                .register(new EksAdapterStub())
                .register(new GkeAdapterStub())
                .register(new AksAdapterStub());
    }
}
