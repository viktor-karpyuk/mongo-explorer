package com.kubrik.mex.k8s.compute.managedpool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * v2.8.4.1-track stub — GKE managed node pool. Mirrors
 * {@link EksAdapterStub}'s contract so the registry can route GCP
 * provider requests without the real GCP SDK on the classpath. The
 * production adapter shipping with v2.8.4.1 swaps this out via
 * {@link ManagedPoolAdapterRegistry#register}.
 */
public final class GkeAdapterStub implements ManagedPoolAdapter {

    private static final Logger log = LoggerFactory.getLogger(GkeAdapterStub.class);
    private final ConcurrentMap<String, PoolDescription> inMemory = new ConcurrentHashMap<>();
    private final AtomicLong seq = new AtomicLong();

    @Override public CloudProvider provider() { return CloudProvider.GCP; }

    @Override
    public PoolOperationResult createPool(CloudCredential credential, ManagedPoolSpec spec) {
        log.info("GKE stub: createPool {} region={} type={}",
                spec.poolName(), spec.region(), spec.instanceType());
        inMemory.put(key(spec.region(), spec.poolName()),
                new PoolDescription(spec.poolName(), PoolPhase.CREATING, 0));
        return PoolOperationResult.accepted("gke-stub-" + seq.incrementAndGet());
    }

    @Override
    public Optional<PoolDescription> describe(CloudCredential credential,
                                                String region, String poolName) {
        PoolDescription d = inMemory.get(key(region, poolName));
        if (d == null) return Optional.empty();
        if (d.phase() == PoolPhase.CREATING) {
            PoolDescription ready = new PoolDescription(poolName, PoolPhase.READY, 3);
            inMemory.put(key(region, poolName), ready);
            return Optional.of(ready);
        }
        return Optional.of(d);
    }

    @Override
    public PoolOperationResult deletePool(CloudCredential credential,
                                            String region, String poolName) {
        log.info("GKE stub: deletePool {} region={}", poolName, region);
        inMemory.remove(key(region, poolName));
        return PoolOperationResult.accepted("gke-stub-" + seq.incrementAndGet());
    }

    private static String key(String region, String name) {
        return region + "/" + name;
    }
}
