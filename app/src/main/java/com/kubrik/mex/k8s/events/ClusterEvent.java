package com.kubrik.mex.k8s.events;

import com.kubrik.mex.k8s.model.ClusterProbeResult;

/**
 * v2.8.1 — Event-bus payload for Kubernetes cluster lifecycle +
 * health. One sealed surface keeps the ClustersPane's status chip
 * updates centralised.
 *
 * <p>Matches the {@code onKubeCluster(clusterId, ClusterEvent)} shape
 * promised in milestone §3.2. {@code clusterId} is the SQLite row id
 * of {@code k8s_clusters}; {@code -1} means "not yet persisted"
 * (emitted by the transient probe preview inside AddClusterDialog).</p>
 */
public sealed interface ClusterEvent {

    long clusterId();

    record Added(long clusterId, String displayName, long at) implements ClusterEvent {}

    record Removed(long clusterId, String displayName, long at) implements ClusterEvent {}

    record Probed(long clusterId, ClusterProbeResult result) implements ClusterEvent {}

    record AuthRefreshFailed(long clusterId, String message, long at) implements ClusterEvent {}
}
