package com.kubrik.mex.k8s.cluster;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.events.ClusterEvent;
import com.kubrik.mex.k8s.model.ClusterProbeResult;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-A4 — Thin service layer wrapping
 * {@link KubeClusterDao}, the client factory, and the probe.
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li>Persist user intent to add / forget a cluster.</li>
 *   <li>Gate destructive actions — we refuse to forget a cluster
 *       while any {@code provisioning_records} row points here (the
 *       schema's RESTRICT FK is a second line; the app check surfaces
 *       a friendly message instead of a raw SQLException).</li>
 *   <li>Publish {@link ClusterEvent}s on the bus so the ClustersPane
 *       refreshes without polling.</li>
 * </ul>
 */
public final class KubeClusterService {

    private static final Logger log = LoggerFactory.getLogger(KubeClusterService.class);

    private final KubeClusterDao dao;
    private final KubeClientFactory clientFactory;
    private final ClusterProbeService probe;
    private final EventBus events;

    public KubeClusterService(KubeClusterDao dao,
                               KubeClientFactory clientFactory,
                               ClusterProbeService probe,
                               EventBus events) {
        this.dao = Objects.requireNonNull(dao, "dao");
        this.clientFactory = Objects.requireNonNull(clientFactory, "clientFactory");
        this.probe = Objects.requireNonNull(probe, "probe");
        this.events = Objects.requireNonNull(events, "events");
    }

    public List<K8sClusterRef> list() throws SQLException {
        return dao.listAll();
    }

    public K8sClusterRef add(String displayName,
                              String kubeconfigPath,
                              String contextName,
                              Optional<String> defaultNamespace,
                              Optional<String> serverUrl) throws SQLException {
        long id = dao.insert(displayName, kubeconfigPath, contextName,
                defaultNamespace, serverUrl);
        K8sClusterRef ref = dao.findById(id).orElseThrow(() ->
                new SQLException("inserted row " + id + " not readable"));
        events.publishKubeCluster(new ClusterEvent.Added(
                ref.id(), ref.displayName(), System.currentTimeMillis()));
        return ref;
    }

    /**
     * Forget a cluster. Refuses when any live provisioning points to it.
     *
     * <p>The check-and-delete runs against the DAO directly. A narrow
     * race is possible: between {@link KubeClusterDao#countLiveProvisions}
     * returning 0 and {@link KubeClusterDao#delete} running, a
     * concurrent Apply could insert a provisioning_records row pointing
     * at this cluster. SQLite's {@code ON DELETE RESTRICT} FK rule then
     * blocks the DELETE with a {@link SQLException}; we catch that and
     * re-surface the friendly {@link IllegalStateException} so the UI
     * still renders a usable message.</p>
     */
    public void remove(long clusterId) throws SQLException {
        Optional<K8sClusterRef> existing = dao.findById(clusterId);
        if (existing.isEmpty()) return;
        int live = dao.countLiveProvisions(clusterId);
        if (live > 0) {
            throw new IllegalStateException("refuse to forget cluster: "
                    + live + " live provisioning record" + (live == 1 ? "" : "s")
                    + " still point here. Tear them down first.");
        }
        try {
            dao.delete(clusterId);
        } catch (SQLException sqle) {
            String msg = sqle.getMessage() == null ? "" : sqle.getMessage().toLowerCase();
            if (msg.contains("foreign key") || msg.contains("constraint")) {
                // Lost the check-and-delete race — a new provisioning row
                // landed between the count and the delete. Translate the
                // raw SQLite error into the same friendly message the
                // deliberate check produces.
                throw new IllegalStateException(
                        "refuse to forget cluster: a provisioning record "
                        + "was inserted concurrently. Retry after tearing it down.");
            }
            throw sqle;
        }
        clientFactory.invalidate(existing.get());
        events.publishKubeCluster(new ClusterEvent.Removed(
                clusterId, existing.get().displayName(), System.currentTimeMillis()));
    }

    /** Probe a cluster and publish the result on the bus. */
    public ClusterProbeResult probe(K8sClusterRef ref) {
        ClusterProbeResult result = probe.probe(ref);
        if (result.ok()) {
            try { dao.touch(ref.id()); } catch (SQLException e) {
                log.debug("touch failed for {}: {}", ref.coordinates(), e.toString());
            }
        }
        events.publishKubeCluster(new ClusterEvent.Probed(ref.id(), result));
        return result;
    }
}
