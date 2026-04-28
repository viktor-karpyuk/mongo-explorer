package com.kubrik.mex.k8s.events;

import com.kubrik.mex.k8s.model.DiscoveredMongo;

import java.util.List;

/**
 * v2.8.1 Q2.8.1-B — Event-bus payload for Mongo workload discovery.
 *
 * <p>Matches milestone §3.2 {@code onDiscovery(clusterId,
 * DiscoveryEvent)}. Current surface is intentionally small: the
 * Clusters pane re-renders the discovered list on {@link Refreshed}
 * and shows a status chip on {@link Failed}. Informer-driven
 * add/remove events land with Q2.8.1-B's informer wiring in a
 * follow-up chunk.</p>
 */
public sealed interface DiscoveryEvent {

    long clusterId();

    record Refreshed(long clusterId, List<DiscoveredMongo> rows, long at) implements DiscoveryEvent {}

    record Failed(long clusterId, String reason, long at) implements DiscoveryEvent {}
}
