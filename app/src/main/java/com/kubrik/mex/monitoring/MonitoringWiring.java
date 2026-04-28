package com.kubrik.mex.monitoring;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.model.ConnectionState;
import com.kubrik.mex.monitoring.sampler.CurrentOpSampler;
import com.kubrik.mex.monitoring.sampler.DbStatsSampler;
import com.kubrik.mex.monitoring.sampler.MetadataSampler;
import com.kubrik.mex.monitoring.sampler.OplogSampler;
import com.kubrik.mex.monitoring.sampler.ReplStatusSampler;
import com.kubrik.mex.monitoring.sampler.Sampler;
import com.kubrik.mex.monitoring.sampler.ServerStatusSampler;
import com.kubrik.mex.monitoring.sampler.ShardingSampler;
import com.kubrik.mex.monitoring.sampler.TopSampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-connection sampler lifecycle: on {@code CONNECTED}, install a default
 * {@link MonitoringProfile} (if one doesn't already exist on disk), then
 * register the workstream-A samplers plus topology samplers. On
 * {@code DISCONNECTED} / {@code ERROR} the {@link MonitoringService} stops the
 * samplers via {@link MonitoringService#disable(String)}.
 *
 * <p>Samplers for topologies that don't apply (e.g. {@code replSetGetStatus}
 * on standalone) silently return empty batches and cost one "first poll"
 * attempt; we register them unconditionally for v2.1.0 rather than probing
 * topology up-front — the cost is negligible and the metadata hasn't been
 * sampled yet when we receive the CONNECTED signal.
 */
public final class MonitoringWiring implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MonitoringWiring.class);

    private final MonitoringService svc;
    private final ConnectionManager connManager;
    /** Connections we've already registered samplers for — idempotency guard. */
    private final Set<String> wired = ConcurrentHashMap.newKeySet();

    public MonitoringWiring(MonitoringService svc, ConnectionManager connManager, EventBus bus) {
        this.svc = svc;
        this.connManager = connManager;
        bus.onState(this::onConnectionState);
    }

    private void onConnectionState(ConnectionState s) {
        switch (s.status()) {
            case CONNECTED -> enableFor(s.connectionId());
            case DISCONNECTED, ERROR -> disableFor(s.connectionId());
            default -> { /* CONNECTING — wait for terminal state */ }
        }
    }

    private void enableFor(String connectionId) {
        if (!wired.add(connectionId)) return;
        MonitoringProfile enabled;
        try {
            MonitoringProfile loaded = svc.profile(connectionId)
                    .orElseGet(() -> MonitoringProfile.defaults(connectionId));
            // The profile may be persisted with enabled=false from a prior disconnect;
            // reconstruct it with enabled=true so registerSampler accepts it.
            enabled = loaded.enabled() ? loaded : new MonitoringProfile(
                    loaded.connectionId(), true,
                    loaded.instancePollInterval(), loaded.storagePollInterval(),
                    loaded.indexUsagePollInterval(), loaded.readPreference(),
                    loaded.profilerEnabled(), loaded.profilerSlowMs(), loaded.profilerAutoDisableAfter(),
                    loaded.topNCollectionsPerDb(), loaded.pinnedCollections(), loaded.retention(),
                    loaded.createdAt(), java.time.Instant.now());
            svc.enable(enabled);
        } catch (SQLException e) {
            log.warn("monitoring: failed to enable for {}", connectionId, e);
            wired.remove(connectionId);
            return;
        }
        MongoService mongo = connManager.service(connectionId);
        if (mongo == null) {
            // No live driver handle yet — the enable() call has persisted the
            // profile with enabled=true, and the later DISCONNECTED event will
            // flip it back via disableFor. Samplers are skipped this cycle.
            return;
        }
        java.util.ArrayList<Sampler> samplers = new java.util.ArrayList<>(List.of(
                ServerStatusSampler.forService(connectionId, mongo),
                new MetadataSampler(connectionId, mongo),
                new DbStatsSampler(connectionId, mongo),
                new ReplStatusSampler(connectionId, mongo),
                new OplogSampler(connectionId, mongo),
                new ShardingSampler(connectionId, mongo),
                new CurrentOpSampler(connectionId, mongo),
                new TopSampler(connectionId, mongo)));
        // If the persisted profile says slow-query profiling was on last session,
        // resume the tailing sampler. (The server-side profiling level itself
        // survives restarts — no re-apply needed here.)
        if (enabled.profilerEnabled()) {
            samplers.add(svc.newProfilerSampler(connectionId, mongo));
        }
        for (Sampler s : samplers) {
            try { svc.registerSampler(s); }
            catch (RuntimeException ex) {
                log.warn("monitoring: register {} failed for {}", s.kind(), connectionId, ex);
            }
        }
    }

    private void disableFor(String connectionId) {
        if (!wired.remove(connectionId)) return;
        try { svc.disable(connectionId); }
        catch (SQLException e) { log.warn("monitoring: disable for {} failed", connectionId, e); }
    }

    @Override
    public void close() {
        wired.clear();
    }
}
