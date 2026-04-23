package com.kubrik.mex.k8s.events;

/**
 * v2.8.1 Q2.8.1-H — Provisioning lifecycle feed.
 *
 * <p>Published at each transition: Started → (Progress*) → Ready /
 * Failed. The rollout viewer subscribes for live updates; the
 * Clusters pane's status chip tracks the current state.</p>
 */
public sealed interface ProvisionEvent {

    long provisioningId();

    record Started(long provisioningId, String deploymentName, long at) implements ProvisionEvent {}
    record Progress(long provisioningId, String step, long at) implements ProvisionEvent {}
    record Ready(long provisioningId, String deploymentName, long at) implements ProvisionEvent {}
    record Failed(long provisioningId, String reason, long at) implements ProvisionEvent {}
}
