package com.kubrik.mex.k8s.compute.managedpool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * v2.8.4 Q2.8.4-B (GA-track stub) — In-memory adapter that proves
 * the v2.8.4 wiring end-to-end without pulling the AWS SDK into the
 * app image before the credentials subsystem is hardened.
 *
 * <p>Every call "succeeds" in-memory, returning a synthetic cloud
 * call id so audit rows land shape-correctly. A {@link #lastOperation}
 * accessor lets tests verify what was passed. The real AWS adapter
 * ships as a follow-on PR once the OS-keychain integration for
 * {@link CloudCredential#keychainRef} is complete + security-reviewed
 * (milestone §1.3). Until then users pick the Managed-pool radio and
 * see a clear "Alpha — simulated cloud adapter active" status in the
 * rollout viewer.</p>
 */
public final class EksAdapterStub implements ManagedPoolAdapter {

    private static final Logger log = LoggerFactory.getLogger(EksAdapterStub.class);
    private final ConcurrentMap<String, PoolDescription> inMemory = new ConcurrentHashMap<>();
    private final AtomicLong seq = new AtomicLong();

    @Override public CloudProvider provider() { return CloudProvider.AWS; }

    @Override
    public PoolOperationResult createPool(CloudCredential credential, ManagedPoolSpec spec) {
        log.info("EKS stub: createPool {} region={} type={}",
                spec.poolName(), spec.region(), spec.instanceType());
        inMemory.put(key(spec.region(), spec.poolName()),
                new PoolDescription(spec.poolName(), PoolPhase.CREATING, 0));
        return PoolOperationResult.accepted("eks-stub-" + seq.incrementAndGet());
    }

    @Override
    public Optional<PoolDescription> describe(CloudCredential credential,
                                                String region, String poolName) {
        PoolDescription d = inMemory.get(key(region, poolName));
        if (d == null) return Optional.empty();
        // Pretend every pool becomes Ready by the second probe.
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
        log.info("EKS stub: deletePool {} region={}", poolName, region);
        inMemory.remove(key(region, poolName));
        return PoolOperationResult.accepted("eks-stub-" + seq.incrementAndGet());
    }

    private static String key(String region, String name) {
        return region + "/" + name;
    }

    /* ============================ test seam ============================ */

    public int operationCount() { return (int) seq.get(); }
    public int liveCount() { return inMemory.size(); }
}
