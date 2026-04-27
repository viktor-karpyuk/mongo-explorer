package com.kubrik.mex.k8s.compute.managedpool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * v2.8.4.2-track stub — AKS agent pool. See {@link EksAdapterStub}
 * for the contract; same shape, different log prefix + cloud-call-id
 * scheme so audit rows land cleanly across the three clouds.
 */
public final class AksAdapterStub implements ManagedPoolAdapter {

    private static final Logger log = LoggerFactory.getLogger(AksAdapterStub.class);
    private final ConcurrentMap<String, PoolDescription> inMemory = new ConcurrentHashMap<>();
    private final AtomicLong seq = new AtomicLong();

    @Override public CloudProvider provider() { return CloudProvider.AZURE; }

    @Override
    public PoolOperationResult createPool(CloudCredential credential, ManagedPoolSpec spec) {
        log.info("AKS stub: createPool {} region={} type={}",
                spec.poolName(), spec.region(), spec.instanceType());
        inMemory.put(key(spec.region(), spec.poolName()),
                new PoolDescription(spec.poolName(), PoolPhase.CREATING, 0));
        return PoolOperationResult.accepted("aks-stub-" + seq.incrementAndGet());
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
        log.info("AKS stub: deletePool {} region={}", poolName, region);
        inMemory.remove(key(region, poolName));
        return PoolOperationResult.accepted("aks-stub-" + seq.incrementAndGet());
    }

    private static String key(String region, String name) {
        return region + "/" + name;
    }
}
