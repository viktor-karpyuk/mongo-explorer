package com.kubrik.mex.security.cert;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.model.ConnectionState;
import com.kubrik.mex.model.MongoConnection;
import com.kubrik.mex.store.ConnectionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * v2.6 Q2.6-E3 — daily sweep: for every connected cluster, resolves
 * its seed host list, opens a TLS handshake via {@link CertFetcher},
 * and upserts the result into {@link CertCacheDao}. Emits one
 * {@link CertExpiryEvent} per connection so the welcome-card chip
 * refreshes.
 *
 * <p>Cadence: one kick on {@link #start} (so the chip hydrates shortly
 * after the first connect), then every
 * {@link #SWEEP_INTERVAL} 24 hours. A single-thread executor keeps the
 * sweeps serialised; per-host fetches run sequentially (dozens of
 * members at most, and each handshake is ≤ 3 s).</p>
 *
 * <p>v2.6.0 parses {@code MongoConnection.hosts} ({@code host:port}
 * comma-separated) for the member list. Walking
 * {@code TopologySnapshot.members[]} so the sweep covers every replset
 * member even after a rotation lands with Q2.6-K.</p>
 */
public final class CertExpiryScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CertExpiryScheduler.class);

    static final Duration SWEEP_INTERVAL = Duration.ofHours(24);
    private static final long AMBER_WINDOW_MS = 30L * 86_400_000L;
    private static final int DEFAULT_PORT = 27017;

    private final ConnectionManager manager;
    private final ConnectionStore store;
    private final CertFetcher fetcher;
    private final CertCacheDao dao;
    private final EventBus events;
    private final Clock clock;
    private final ScheduledExecutorService exec;

    private ScheduledFuture<?> scheduled;
    private volatile boolean closed = false;

    public CertExpiryScheduler(ConnectionManager manager, ConnectionStore store,
                                 CertFetcher fetcher, CertCacheDao dao,
                                 EventBus events, Clock clock) {
        this.manager = manager;
        this.store = store;
        this.fetcher = fetcher;
        this.dao = dao;
        this.events = events;
        this.clock = clock == null ? Clock.systemUTC() : clock;
        this.exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cert-expiry-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        // Kick the first sweep 30 s after start so the welcome card
        // hydrates shortly after connects, then every 24 h.
        scheduled = exec.scheduleAtFixedRate(this::safeSweep,
                30, SWEEP_INTERVAL.toSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        closed = true;
        if (scheduled != null) scheduled.cancel(false);
        exec.shutdownNow();
    }

    /** Visible for tests — runs one sweep synchronously on the caller's thread. */
    void sweepOnce() {
        if (closed) return;
        for (MongoConnection c : store.list()) {
            if (manager.state(c.id()).status() != ConnectionState.Status.CONNECTED) continue;
            sweepConnection(c);
        }
    }

    private void safeSweep() {
        try { sweepOnce(); }
        catch (Throwable t) {
            log.warn("cert-expiry sweep failed: {}", t.getMessage(), t);
        }
    }

    /** Sweep a single connection. Package-private so the Security tab's
     *  'Refresh' action can trigger a manual sweep without waiting 24h. */
    void sweepConnection(MongoConnection c) {
        List<String> members = parseMembers(c);
        if (members.isEmpty()) {
            log.debug("cert sweep {}: no parseable hosts", c.id());
            return;
        }
        long nowMs = clock.millis();
        int fetched = 0;
        for (String member : members) {
            String host = member;
            int port = DEFAULT_PORT;
            int colon = member.lastIndexOf(':');
            if (colon > 0) {
                host = member.substring(0, colon);
                try { port = Integer.parseInt(member.substring(colon + 1)); }
                catch (NumberFormatException ignored) {}
            }
            List<CertRecord> certs = fetcher.fetch(host, port);
            for (CertRecord cert : certs) {
                try { dao.upsert(c.id(), cert, nowMs); fetched++; }
                catch (Exception ex) {
                    log.debug("cert upsert {} on {}: {}", c.id(), member, ex.getMessage());
                }
            }
        }
        int expired = 0, soon = 0;
        for (CertCacheDao.Row row : dao.listForConnection(c.id())) {
            if (row.notAfter() == null) continue;
            long remaining = row.notAfter() - nowMs;
            if (remaining < 0) expired++;
            else if (remaining < AMBER_WINDOW_MS) soon++;
        }
        events.publishCertExpiry(new CertExpiryEvent(c.id(), nowMs, expired, soon));
        log.info("cert sweep {} : {} hosts probed, {} certs cached, {} expired, {} expiring",
                c.id(), members.size(), fetched, expired, soon);
    }

    /* ============================= helpers ============================= */

    static List<String> parseMembers(MongoConnection c) {
        // FORM-mode connections carry a comma-separated host:port list.
        // URI + DNS-SRV modes defer until Q2.6-K can synthesise the
        // members from the driver's TopologyDescription.
        if (c == null) return List.of();
        if (!"FORM".equalsIgnoreCase(c.mode())) return List.of();
        if (c.hosts() == null || c.hosts().isBlank()) return List.of();
        List<String> out = new ArrayList<>();
        for (String raw : c.hosts().split(",")) {
            String h = raw.trim();
            if (!h.isEmpty()) out.add(h);
        }
        return out;
    }
}
