package com.kubrik.mex.core;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.model.ConnectionState;
import com.kubrik.mex.model.MongoConnection;
import com.kubrik.mex.store.ConnectionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionManager {

    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    private final ConnectionStore store;
    private final EventBus events;
    private final Crypto crypto;
    private final Map<String, MongoService> active = new ConcurrentHashMap<>();
    private final Map<String, ConnectionState> states = new ConcurrentHashMap<>();

    public ConnectionManager(ConnectionStore store, EventBus events, Crypto crypto) {
        this.store = store;
        this.events = events;
        this.crypto = crypto;
    }

    public Crypto crypto() { return crypto; }

    public ConnectionState state(String id) {
        return states.getOrDefault(id, ConnectionState.disconnected(id));
    }

    public MongoService service(String id) { return active.get(id); }

    public void connect(String id) {
        MongoConnection c = store.get(id);
        if (c == null) return;
        if (c.sshEnabled()) {
            publish(new ConnectionState(id, ConnectionState.Status.ERROR, null,
                    "SSH tunnel is not yet supported"));
            return;
        }
        publish(new ConnectionState(id, ConnectionState.Status.CONNECTING, null, null));
        events.publishLog(id, "connecting to " + c.name() + "…");
        String uri = ConnectionUriBuilder.build(c, crypto);
        Thread.startVirtualThread(() -> {
            try {
                MongoService svc = new MongoService(uri);
                // Atomic replace — if a concurrent second connect()
                // (user double-click, or a retry racing the previous
                // attempt) wins and writes first, we close the losing
                // client here rather than letting it linger to JVM
                // exit.
                MongoService prior = active.put(id, svc);
                if (prior != null && prior != svc) {
                    try { prior.close(); } catch (Exception ignored) {}
                }
                publish(new ConnectionState(id, ConnectionState.Status.CONNECTED, svc.serverVersion(), null));
                events.publishLog(id, "connected to " + c.name() + " (mongo " + svc.serverVersion() + ")");
            } catch (Exception e) {
                log.warn("connect failed: {}", e.toString());
                events.publishLog(id, "ERROR " + describe(e));
                publish(new ConnectionState(id, ConnectionState.Status.ERROR, null, describe(e)));
            }
        });
    }

    public void disconnect(String id) {
        MongoService svc = active.remove(id);
        if (svc != null) {
            // Close off the calling thread — MongoClient.close() can block
            Thread.startVirtualThread(() -> { try { svc.close(); } catch (Exception ignored) {} });
        }
        publish(ConnectionState.disconnected(id));
    }

    public String testConnection(MongoConnection c) {
        if (c.sshEnabled()) throw new RuntimeException("SSH tunnel is not yet supported");
        String uri = ConnectionUriBuilder.build(c, crypto);
        try (MongoService svc = new MongoService(uri)) {
            return svc.serverVersion();
        }
    }

    public void closeAll() {
        for (MongoService s : active.values()) {
            try { s.close(); } catch (Exception ignored) {}
        }
        active.clear();
    }

    private static String describe(Throwable t) {
        StringBuilder sb = new StringBuilder();
        Throwable cur = t;
        while (cur != null) {
            if (sb.length() > 0) sb.append("  ↳ ");
            sb.append(cur.getClass().getSimpleName()).append(": ").append(cur.getMessage());
            cur = cur.getCause();
            if (cur == t) break;
        }
        return sb.toString();
    }

    private void publish(ConnectionState s) {
        states.put(s.connectionId(), s);
        events.publishState(s);
    }
}
