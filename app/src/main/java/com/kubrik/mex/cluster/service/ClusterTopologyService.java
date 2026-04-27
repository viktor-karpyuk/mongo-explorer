package com.kubrik.mex.cluster.service;

import com.kubrik.mex.cluster.model.ClusterKind;
import com.kubrik.mex.cluster.model.Member;
import com.kubrik.mex.cluster.model.MemberState;
import com.kubrik.mex.cluster.model.Mongos;
import com.kubrik.mex.cluster.model.Shard;
import com.kubrik.mex.cluster.model.TopologySnapshot;
import com.kubrik.mex.cluster.store.TopologySnapshotDao;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.events.EventBus;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * v2.4 TOPO-* — per-connection cluster topology sampler.
 *
 * <p>Each started connection runs on a single-threaded scheduled executor
 * that ticks at 300 ms, coalesces into a 2 s visible cadence, enforces a
 * 5 s {@code maxTimeMS} per command, and backs off exponentially on failure
 * (2 → 4 → 8 → 16 s cap). Snapshots are persisted every 60 s if their
 * {@code sha256} differs from the most recent row; emission to the bus
 * happens on every visible tick.</p>
 */
public final class ClusterTopologyService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClusterTopologyService.class);

    private static final long HEARTBEAT_MS = 300;
    private static final long VISIBLE_MS   = 2_000;
    private static final long PERSIST_MS   = 60_000;
    private static final int  COMMAND_TIMEOUT_MS = 5_000;
    private static final long BACKOFF_INITIAL_MS = 2_000;
    private static final long BACKOFF_MAX_MS     = 16_000;

    private final Function<String, MongoService> serviceLookup;
    private final TopologySnapshotDao dao;
    private final EventBus bus;
    private final Clock clock;

    private final ConcurrentMap<String, State> states = new ConcurrentHashMap<>();
    /** Per-shard MongoClient cache, keyed by {@code connectionId + "||" + rsHostSpec}.
     *  Clients are opened lazily on the first sharded sample and closed when the
     *  service shuts down or a connection stops; changing shard topology (new seed
     *  list) swaps the entry atomically. */
    private final ConcurrentMap<String, MongoClient> shardClients = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    public ClusterTopologyService(Function<String, MongoService> serviceLookup,
                                  TopologySnapshotDao dao, EventBus bus, Clock clock) {
        this.serviceLookup = Objects.requireNonNull(serviceLookup);
        this.dao = dao;
        this.bus = bus;
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    /** Begin sampling for a connection. Idempotent — safe to call repeatedly. */
    public void start(String connectionId) {
        if (closed) return;
        states.computeIfAbsent(connectionId, this::newState);
    }

    /** Stop sampling for a connection (keeps persisted snapshots untouched). */
    public void stop(String connectionId) {
        State s = states.remove(connectionId);
        if (s != null) s.close();
        closeShardClientsFor(connectionId);
    }

    /** Force one immediate sample, bypassing back-off. */
    public TopologySnapshot refreshNow(String connectionId) {
        State s = states.get(connectionId);
        if (s == null) return null;
        return s.sampleOnce(true);
    }

    /** Latest in-memory snapshot; {@code null} if the service never produced one. */
    public TopologySnapshot latest(String connectionId) {
        State s = states.get(connectionId);
        return s == null ? null : s.latest;
    }

    @Override
    public void close() {
        closed = true;
        for (State s : states.values()) s.close();
        states.clear();
        for (MongoClient c : shardClients.values()) {
            try { c.close(); } catch (Exception ignored) {}
        }
        shardClients.clear();
    }

    private void closeShardClientsFor(String connectionId) {
        String prefix = connectionId + "||";
        shardClients.entrySet().removeIf(e -> {
            if (!e.getKey().startsWith(prefix)) return false;
            try { e.getValue().close(); } catch (Exception ignored) {}
            return true;
        });
    }

    /* ================================================================== */

    private State newState(String connectionId) {
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "topology-" + connectionId);
            t.setDaemon(true);
            return t;
        });
        State s = new State(connectionId, exec);
        try {
            ScheduledFuture<?> tick = exec.scheduleAtFixedRate(s::heartbeat,
                    HEARTBEAT_MS, HEARTBEAT_MS, TimeUnit.MILLISECONDS);
            s.tick = tick;
            return s;
        } catch (RuntimeException re) {
            // RejectedExecutionException would only fire on a pre-shut
            // executor — defensive belt-and-braces in case someone
            // wraps the factory later. Without this, a failed schedule
            // leaks the unexecuted ScheduledExecutorService for the
            // JVM's lifetime.
            exec.shutdownNow();
            throw re;
        }
    }

    /** Per-connection state + tick loop. */
    private final class State {
        final String connectionId;
        final ScheduledExecutorService exec;
        ScheduledFuture<?> tick;

        volatile TopologySnapshot latest;
        volatile long nextVisibleAt = 0L;
        volatile long nextPersistAt = 0L;
        volatile long backoffUntil = 0L;
        volatile long backoffDelay = BACKOFF_INITIAL_MS;
        volatile int  consecutiveFailures = 0;

        State(String connectionId, ScheduledExecutorService exec) {
            this.connectionId = connectionId;
            this.exec = exec;
        }

        void heartbeat() {
            long now = clock.millis();
            if (now < backoffUntil) return;
            if (now < nextVisibleAt) return;
            sampleOnce(false);
        }

        TopologySnapshot sampleOnce(boolean force) {
            long now = clock.millis();
            MongoService svc = serviceLookup.apply(connectionId);
            if (svc == null) {
                scheduleBackoff();
                return null;
            }
            TopologySnapshot snap;
            try {
                snap = sample(connectionId, svc, now);
                consecutiveFailures = 0;
                backoffDelay = BACKOFF_INITIAL_MS;
                backoffUntil = 0L;
            } catch (Exception e) {
                log.debug("topology sample failed for {}: {}", connectionId, e.getMessage());
                scheduleBackoff();
                return null;
            }
            latest = snap;
            nextVisibleAt = now + VISIBLE_MS;
            if (bus != null) bus.publishTopology(connectionId, snap);
            if (dao != null && (force || now >= nextPersistAt)) {
                dao.insertIfChanged(connectionId, snap);
                nextPersistAt = now + PERSIST_MS;
            }
            return snap;
        }

        void scheduleBackoff() {
            consecutiveFailures++;
            long now = clock.millis();
            backoffUntil = now + backoffDelay;
            backoffDelay = Math.min(backoffDelay * 2, BACKOFF_MAX_MS);
        }

        void close() {
            if (tick != null) tick.cancel(false);
            exec.shutdownNow();
        }
    }

    /* ==================== command orchestration ======================== */

    private TopologySnapshot sample(String connectionId, MongoService svc, long capturedAt) {
        Document hello = runAdmin(svc, new Document("hello", 1));
        ClusterKind kind = detectKind(hello);
        String version = svc.serverVersion();

        return switch (kind) {
            case STANDALONE -> new TopologySnapshot(ClusterKind.STANDALONE, capturedAt, version,
                    List.of(Member.unknownAt(hostOf(hello, "me"))),
                    List.of(), List.of(), List.of(), List.of());
            case REPLSET   -> sampleReplica(svc, capturedAt, version);
            case SHARDED   -> sampleSharded(connectionId, svc, capturedAt, version);
        };
    }

    private TopologySnapshot sampleReplica(MongoService svc, long capturedAt, String version) {
        Document status = runAdmin(svc, new Document("replSetGetStatus", 1));
        Document config = safeRunAdmin(svc, new Document("replSetGetConfig", 1));
        List<Member> members = buildMembers(status, config);
        return new TopologySnapshot(ClusterKind.REPLSET, capturedAt, version,
                members, List.of(), List.of(), List.of(), List.of());
    }

    private TopologySnapshot sampleSharded(String connectionId, MongoService svc,
                                           long capturedAt, String version) {
        List<String> warnings = new ArrayList<>();
        Document shardsCmd = safeRunAdmin(svc, new Document("listShards", 1));
        if (shardsCmd == null) warnings.add("listShards unavailable");
        List<Shard> shards = buildShards(shardsCmd);
        Document shardMap = safeRunAdmin(svc, new Document("getShardMap", 1));
        List<Member> configServers = buildConfigServers(shardMap);
        if (configServers.isEmpty()) warnings.add("config-server topology unavailable");
        List<Mongos> mongos = buildMongos(svc);

        // Probe each shard directly for live member state (Q2.4-A follow-up:
        // without this, shard members render as UNKNOWN grey cards). A failure
        // for one shard surfaces as a warning; other shards still render.
        List<Shard> probed = new ArrayList<>(shards.size());
        for (Shard s : shards) {
            probed.add(probeShardMembers(connectionId, svc, s, warnings));
        }
        return new TopologySnapshot(ClusterKind.SHARDED, capturedAt, version,
                List.of(), probed, mongos, configServers, warnings);
    }

    /** Runs {@code replSetGetStatus} / {@code replSetGetConfig} against the
     *  shard's dedicated client, returning a new {@link Shard} with live
     *  members. Falls back to the seed-host UNKNOWN members on any failure
     *  and records a warning so the UI's warnings banner surfaces it. */
    private Shard probeShardMembers(String connectionId, MongoService svc,
                                    Shard shard, List<String> warnings) {
        String rsHostSpec = shard.rsHost();
        if (rsHostSpec == null || rsHostSpec.isBlank()) return shard;
        String key = connectionId + "||" + rsHostSpec;
        MongoClient peer = shardClients.computeIfAbsent(key, k -> {
            try { return svc.openPeerClient(rsHostSpec, COMMAND_TIMEOUT_MS); }
            catch (Exception e) {
                log.debug("openPeerClient failed for shard {}: {}", shard.id(), e.getMessage());
                return null;
            }
        });
        if (peer == null) {
            warnings.add("shard " + shard.id() + " unreachable");
            return shard;
        }
        try {
            MongoDatabase admin = peer.getDatabase("admin");
            Document cmd = new Document("replSetGetStatus", 1)
                    .append("maxTimeMS", COMMAND_TIMEOUT_MS);
            Document status = admin.runCommand(cmd);
            Document configReply = null;
            try {
                configReply = admin.runCommand(new Document("replSetGetConfig", 1)
                        .append("maxTimeMS", COMMAND_TIMEOUT_MS));
            } catch (Exception ignored) { /* config is optional for lag calc */ }
            List<Member> liveMembers = buildMembers(status, configReply);
            return new Shard(shard.id(), shard.rsHost(), shard.draining(),
                    shard.tags(), liveMembers);
        } catch (Exception e) {
            log.debug("probe shard {} failed: {}", shard.id(), e.getMessage());
            warnings.add("shard " + shard.id() + " probe failed: " + e.getClass().getSimpleName());
            // Rotate the cached client — next tick will re-open.
            MongoClient stale = shardClients.remove(key);
            if (stale != null) try { stale.close(); } catch (Exception ignored) {}
            return shard;
        }
    }

    /* ==================== parsing helpers ============================== */

    private static ClusterKind detectKind(Document hello) {
        String msg = hello.getString("msg");
        if ("isdbgrid".equals(msg)) return ClusterKind.SHARDED;
        Object setName = hello.get("setName");
        if (setName instanceof String s && !s.isBlank()) return ClusterKind.REPLSET;
        return ClusterKind.STANDALONE;
    }

    @SuppressWarnings("unchecked")
    private static List<Member> buildMembers(Document status, Document config) {
        List<Document> statusMembers = (List<Document>) status.get("members");
        if (statusMembers == null) return List.of();
        Map<String, Document> cfgByHost = new HashMap<>();
        if (config != null) {
            Document cfg = (Document) config.get("config");
            if (cfg != null) {
                List<Document> cfgMembers = (List<Document>) cfg.get("members");
                if (cfgMembers != null) for (Document m : cfgMembers) {
                    cfgByHost.put(m.getString("host"), m);
                }
            }
        }
        long primaryOpt = 0L;
        for (Document m : statusMembers) {
            if ("PRIMARY".equals(m.getString("stateStr"))) {
                Date d = (Date) m.get("optimeDate");
                if (d != null) primaryOpt = d.getTime();
                break;
            }
        }
        List<Member> out = new ArrayList<>(statusMembers.size());
        for (Document m : statusMembers) {
            String host = m.getString("name");
            MemberState state = MemberState.from(
                    m.getInteger("state"), m.getString("stateStr"));
            Date optDate = (Date) m.get("optimeDate");
            Long optimeMs = optDate == null ? null : optDate.getTime();
            Long lagMs = (state == MemberState.PRIMARY || optimeMs == null || primaryOpt == 0)
                    ? null : Math.max(0, primaryOpt - optimeMs);
            Document cfg = cfgByHost.get(host);
            Integer priority = cfg == null ? null : intOrNull(cfg.get("priority"));
            Integer votes    = cfg == null ? null : intOrNull(cfg.get("votes"));
            Boolean hidden   = cfg == null ? null : boolOrNull(cfg.get("hidden"));
            Boolean arbiter  = cfg == null ? null : boolOrNull(cfg.get("arbiterOnly"));
            Map<String, String> tags = cfg == null ? Map.of() : extractTags(cfg);

            Integer configVersion = intOrNull(m.get("configVersion"));
            out.add(new Member(host, state, priority, votes, hidden, arbiter, tags,
                    optimeMs, lagMs,
                    longOrNull(m.get("pingMs")),
                    longOrNull(m.get("uptime")),
                    m.getString("syncSourceHost"),
                    configVersion));
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static List<Shard> buildShards(Document shardsCmd) {
        if (shardsCmd == null) return List.of();
        List<Document> entries = (List<Document>) shardsCmd.get("shards");
        if (entries == null) return List.of();
        List<Shard> out = new ArrayList<>(entries.size());
        for (Document s : entries) {
            String host = s.getString("host");
            List<Member> members = parseSeedHosts(host);
            out.add(new Shard(
                    s.getString("_id"),
                    host == null ? "" : host,
                    Boolean.TRUE.equals(s.getBoolean("draining")),
                    Map.of(),
                    members
            ));
        }
        return out;
    }

    /** Parses the {@code rsName/host1:port,host2:port} shape returned by
     *  {@code listShards} into a list of {@code UNKNOWN}-state members. Live
     *  state is populated later by a dedicated per-shard probe. */
    private static List<Member> parseSeedHosts(String host) {
        if (host == null || host.isBlank()) return List.of();
        int slash = host.indexOf('/');
        String seeds = slash < 0 ? host : host.substring(slash + 1);
        List<Member> out = new ArrayList<>();
        for (String seed : seeds.split(",")) {
            String h = seed.trim();
            if (!h.isEmpty()) out.add(Member.unknownAt(h));
        }
        return out;
    }

    /** Extracts the config-server host list from {@code getShardMap}. The map
     *  key {@code "config"} holds {@code rsName/host1:port,host2:port,...}. */
    private static List<Member> buildConfigServers(Document shardMap) {
        if (shardMap == null) return List.of();
        Object rawMap = shardMap.get("map");
        if (!(rawMap instanceof Document m)) return List.of();
        Object config = m.get("config");
        if (!(config instanceof String s) || s.isBlank()) return List.of();
        return parseSeedHosts(s);
    }

    /** Reads {@code config.mongos} for the live mongos roster. The collection
     *  is maintained by each mongos pinging at ~30 s cadence, so entries older
     *  than 60 s are treated as stale and filtered out. */
    @SuppressWarnings("unchecked")
    private static List<Mongos> buildMongos(MongoService svc) {
        List<Mongos> out = new ArrayList<>();
        try {
            var coll = svc.database("config").getCollection("mongos");
            long freshnessMs = 60_000;
            long now = System.currentTimeMillis();
            for (Document d : coll.find()) {
                String id = d.getString("_id");
                if (id == null) continue;
                Date ping = (Date) d.get("ping");
                if (ping != null && now - ping.getTime() > freshnessMs) continue;
                Long up = longOrNull(d.get("up"));
                Long delay = longOrNull(d.get("waiting"));
                String mv = d.getString("mongoVersion");
                out.add(new Mongos(id, mv == null ? "" : mv, up, delay));
            }
        } catch (Exception ignored) { /* partial topology is ok */ }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> extractTags(Document cfg) {
        Object tags = cfg.get("tags");
        if (!(tags instanceof Document d)) return Map.of();
        Map<String, String> out = new HashMap<>();
        for (Map.Entry<String, Object> e : d.entrySet()) {
            if (e.getValue() != null) out.put(e.getKey(), String.valueOf(e.getValue()));
        }
        return out;
    }

    private static String hostOf(Document hello, String key) {
        Object v = hello.get(key);
        return v == null ? "" : v.toString();
    }

    private static Integer intOrNull(Object o) {
        if (o instanceof Number n) return n.intValue();
        return null;
    }

    private static Long longOrNull(Object o) {
        if (o instanceof Number n) return n.longValue();
        return null;
    }

    private static Boolean boolOrNull(Object o) {
        return o instanceof Boolean b ? b : null;
    }

    /* ==================== mongo helpers ================================ */

    private static Document runAdmin(MongoService svc, Document cmd) {
        cmd.put("maxTimeMS", COMMAND_TIMEOUT_MS);
        return svc.database("admin").runCommand(cmd);
    }

    /** Returns {@code null} instead of throwing on benign refusals (standalone, missing privileges). */
    private static Document safeRunAdmin(MongoService svc, Document cmd) {
        try {
            return runAdmin(svc, cmd);
        } catch (MongoCommandException mce) {
            return null;
        } catch (Exception e) {
            return null;
        }
    }
}
