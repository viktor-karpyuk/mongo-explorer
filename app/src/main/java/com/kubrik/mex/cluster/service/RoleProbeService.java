package com.kubrik.mex.cluster.service;

import com.kubrik.mex.cluster.safety.RoleSet;
import com.kubrik.mex.cluster.store.RoleCacheDao;
import com.kubrik.mex.core.MongoService;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * v2.4 ROLE-1..4 — per-connection role cache with on-demand refresh.
 *
 * <p>Destructive buttons call {@link #currentOrProbe} synchronously; if the
 * cache is older than {@link #STALE_AFTER} the call reprobes within a ≤ 2 s
 * budget. Explicit invalidation happens after any destructive failure with
 * a role-related server message.</p>
 */
public final class RoleProbeService {

    private static final Logger log = LoggerFactory.getLogger(RoleProbeService.class);

    static final Duration STALE_AFTER = Duration.ofSeconds(60);
    static final Duration REFRESH_EVERY = Duration.ofMinutes(5);
    static final Duration PROBE_TIMEOUT = Duration.ofSeconds(2);

    private final Function<String, MongoService> serviceLookup;
    private final RoleCacheDao dao;
    private final Clock clock;

    private final ConcurrentMap<String, RoleCacheDao.Cached> inMemory = new ConcurrentHashMap<>();

    public RoleProbeService(Function<String, MongoService> serviceLookup, RoleCacheDao dao, Clock clock) {
        this.serviceLookup = Objects.requireNonNull(serviceLookup);
        this.dao = dao;
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    /** Returns the cached role set if still fresh, otherwise synchronously reprobes. */
    public RoleSet currentOrProbe(String connectionId) {
        RoleCacheDao.Cached cached = inMemory.get(connectionId);
        if (cached == null && dao != null) cached = dao.load(connectionId);
        if (cached != null && !isStale(cached.probedAtMs())) {
            inMemory.putIfAbsent(connectionId, cached);
            return cached.roles();
        }
        return probe(connectionId);
    }

    /** Force a probe now, bypassing the cache. Returns {@link RoleSet#EMPTY} on failure. */
    public RoleSet probe(String connectionId) {
        MongoService svc = serviceLookup.apply(connectionId);
        if (svc == null) return RoleSet.EMPTY;
        try {
            Document cmd = new Document("connectionStatus", 1)
                    .append("showPrivileges", true)
                    .append("maxTimeMS", (int) PROBE_TIMEOUT.toMillis());
            Document reply = svc.database("admin").runCommand(cmd);
            RoleSet roles = RoleSet.parse(reply);
            long now = clock.millis();
            RoleCacheDao.Cached fresh = new RoleCacheDao.Cached(roles, now);
            inMemory.put(connectionId, fresh);
            if (dao != null) dao.upsert(connectionId, roles, now);
            return roles;
        } catch (Exception e) {
            log.debug("role probe failed for {}: {}", connectionId, e.getMessage());
            return RoleSet.EMPTY;
        }
    }

    /** Invalidate (e.g., after a destructive failure indicating role drift). */
    public void invalidate(String connectionId) {
        inMemory.remove(connectionId);
        if (dao != null) dao.invalidate(connectionId);
    }

    /** Visible for tests: is this entry older than {@link #STALE_AFTER}? */
    boolean isStale(long probedAtMs) {
        return clock.millis() - probedAtMs > STALE_AFTER.toMillis();
    }
}
