package com.kubrik.mex.cluster;

import com.kubrik.mex.cluster.service.ClusterTopologyService;
import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.model.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * v2.4 — per-connection lifecycle for {@link ClusterTopologyService}. Mirrors
 * the {@code MonitoringWiring} pattern: start the topology sampler on
 * {@code CONNECTED}, stop it on {@code DISCONNECTED} / {@code ERROR}. Also
 * clears the replay cache in {@link EventBus#clearTopologyFor} so reconnect
 * doesn't deliver stale data to late subscribers.
 */
public final class ClusterWiring implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClusterWiring.class);

    private final ClusterTopologyService topology;
    private final EventBus bus;
    private final Set<String> wired = ConcurrentHashMap.newKeySet();
    private final EventBus.Subscription sub;

    public ClusterWiring(ClusterTopologyService topology,
                         ConnectionManager connManager, EventBus bus) {
        this.topology = topology;
        this.bus = bus;
        this.sub = bus.onState(this::onConnectionState);
    }

    private void onConnectionState(ConnectionState s) {
        switch (s.status()) {
            case CONNECTED -> enableFor(s.connectionId());
            case DISCONNECTED, ERROR -> disableFor(s.connectionId());
            default -> { /* CONNECTING */ }
        }
    }

    private void enableFor(String connectionId) {
        if (!wired.add(connectionId)) return;
        try {
            topology.start(connectionId);
        } catch (RuntimeException e) {
            log.warn("cluster: topology start failed for {}", connectionId, e);
            wired.remove(connectionId);
        }
    }

    private void disableFor(String connectionId) {
        if (!wired.remove(connectionId)) return;
        try { topology.stop(connectionId); } catch (RuntimeException ignored) {}
        bus.clearTopologyFor(connectionId);
    }

    @Override
    public void close() {
        try { sub.close(); } catch (Exception ignored) {}
        for (String id : wired) {
            try { topology.stop(id); } catch (RuntimeException ignored) {}
        }
        wired.clear();
    }
}
