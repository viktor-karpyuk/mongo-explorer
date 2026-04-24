package com.kubrik.mex.k8s.compute.managedpool;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.4 Q2.8.4-B — Per-cloud managed-pool adapter.
 *
 * <p>Implementations wrap the cloud's SDK: {@code eks:CreateNodegroup}
 * for AWS, {@code container.nodePools.create} for GCP, {@code
 * agentpools.createOrUpdate} for Azure. Each operation is blocking —
 * callers run them on a virtual thread — and returns a
 * {@link PoolOperationResult} that carries the cloud-side call id so
 * audit rows in {@code managed_pool_operations} can round-trip to
 * CloudTrail / Audit Logs / Activity Logs.</p>
 *
 * <p>The interface is deliberately small: v2.8.4 GA creates + deletes
 * pools and nothing else. Resize / upgrade / cordon are NG-2.8.4-3.</p>
 */
public interface ManagedPoolAdapter {

    CloudProvider provider();

    /** Start pool creation. Returns as soon as the cloud API accepts
     *  the request; the caller polls {@link #describe} until Ready. */
    PoolOperationResult createPool(CloudCredential credential, ManagedPoolSpec spec);

    /** Fetch the current status of a pool the adapter previously
     *  created. Returns {@link Optional#empty()} for a pool that no
     *  longer exists (cloud-side deletion, manual teardown). */
    Optional<PoolDescription> describe(CloudCredential credential,
                                        String region, String poolName);

    /** Tear the pool down. Returns as soon as the cloud API accepts;
     *  callers poll {@link #describe} to confirm removal. */
    PoolOperationResult deletePool(CloudCredential credential,
                                    String region, String poolName);

    /** A single-API-call record the audit table persists. */
    record PoolOperationResult(
            Status status,
            Optional<String> cloudCallId,
            Optional<String> errorMessage
    ) {
        public PoolOperationResult {
            Objects.requireNonNull(status, "status");
            cloudCallId = cloudCallId == null ? Optional.empty() : cloudCallId;
            errorMessage = errorMessage == null ? Optional.empty() : errorMessage;
        }

        public static PoolOperationResult accepted(String callId) {
            return new PoolOperationResult(Status.ACCEPTED,
                    Optional.ofNullable(callId), Optional.empty());
        }
        public static PoolOperationResult rejected(String reason) {
            return new PoolOperationResult(Status.REJECTED,
                    Optional.empty(), Optional.ofNullable(reason));
        }

        public enum Status { ACCEPTED, REJECTED }
    }

    record PoolDescription(
            String poolName,
            PoolPhase phase,
            int readyNodeCount
    ) {}

    enum PoolPhase { CREATING, READY, UPDATING, DELETING, FAILED, ABSENT }
}
