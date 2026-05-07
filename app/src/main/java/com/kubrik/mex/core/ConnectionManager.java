package com.kubrik.mex.core;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.model.ConnectionState;
import com.kubrik.mex.model.MongoConnection;
import com.kubrik.mex.store.ConnectionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConnectionManager {

    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    private final ConnectionStore store;
    private final EventBus events;
    private final Crypto crypto;
    private final Map<String, MongoService> active = new ConcurrentHashMap<>();
    private final Map<String, ConnectionState> states = new ConcurrentHashMap<>();
    /** Per-id "settled" flag for the most-recent in-flight connect.
     *  cancelConnect flips this to abort the attempt. */
    private final Map<String, AtomicBoolean> attempts = new ConcurrentHashMap<>();

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

    /** Hard upper bound on the connect attempt — slightly above the
     *  driver's 30 s server-selection timeout. If the driver itself
     *  ever hangs past this (which has happened with broken DNS / TCP
     *  black-holes), the watchdog publishes ERROR so the UI never sits
     *  on a CONNECTING spinner forever. */
    private static final long CONNECT_WATCHDOG_MS = 35_000;

    public void connect(String id) {
        // Run the entire connect path off the calling thread so the FX
        // thread can never block on store I/O, keychain decrypt, or
        // synchronous EventBus listeners. Always publish a terminal state
        // on every exit so UI feedback can detach its one-shot
        // subscription.
        AtomicBoolean settled = new AtomicBoolean(false);
        // Replace any previous attempt's settle flag — if the user is
        // restarting the connect (e.g., after Cancel), the older attempt
        // can no longer be cancelled but its terminal state is already
        // discarded by its own settled flag.
        attempts.put(id, settled);
        Thread.startVirtualThread(() -> {
            MongoConnection c;
            try {
                c = store.get(id);
            } catch (Exception e) {
                log.warn("connect: store.get({}) failed", id, e);
                publishTerminal(settled, new ConnectionState(id, ConnectionState.Status.ERROR, null, describe(e)));
                return;
            }
            if (c == null) {
                publishTerminal(settled, new ConnectionState(id, ConnectionState.Status.ERROR, null, "connection not found"));
                return;
            }
            if (c.sshEnabled()) {
                publishTerminal(settled, new ConnectionState(id, ConnectionState.Status.ERROR, null,
                        "SSH tunnel is not yet supported"));
                return;
            }
            publish(new ConnectionState(id, ConnectionState.Status.CONNECTING, null, null));
            events.publishLog(id, "connecting to " + c.name() + "…");
            String uri;
            try {
                uri = ConnectionUriBuilder.build(c, crypto);
            } catch (Exception e) {
                log.warn("connect: URI build failed for {}", id, e);
                events.publishLog(id, "ERROR " + describe(e));
                publishTerminal(settled, new ConnectionState(id, ConnectionState.Status.ERROR, null, describe(e)));
                return;
            }
            // Watchdog: forces a terminal ERROR if the driver hangs past
            // CONNECT_WATCHDOG_MS. Cancels itself implicitly via
            // `settled` once we publish a real CONNECTED/ERROR.
            String connName = c.name();
            Thread.startVirtualThread(() -> {
                try { Thread.sleep(CONNECT_WATCHDOG_MS); } catch (InterruptedException ignored) { return; }
                if (settled.get()) return;
                String msg = "Timed out after " + (CONNECT_WATCHDOG_MS / 1000) + " s while connecting to " + connName;
                events.publishLog(id, "ERROR " + msg);
                publishTerminal(settled, new ConnectionState(id, ConnectionState.Status.ERROR, null, msg));
            });
            try {
                MongoService svc = new MongoService(uri);
                if (settled.get()) {
                    // The watchdog already declared timeout — discard the
                    // late client so it doesn't leak.
                    try { svc.close(); } catch (Exception ignored) {}
                    return;
                }
                // Atomic replace — if a concurrent second connect()
                // (user double-click, or a retry racing the previous
                // attempt) wins and writes first, we close the losing
                // client here rather than letting it linger to JVM
                // exit.
                MongoService prior = active.put(id, svc);
                if (prior != null && prior != svc) {
                    try { prior.close(); } catch (Exception ignored) {}
                }
                publishTerminal(settled, new ConnectionState(id, ConnectionState.Status.CONNECTED, svc.serverVersion(), null));
                events.publishLog(id, "connected to " + c.name() + " (mongo " + svc.serverVersion() + ")");
            } catch (Exception e) {
                log.warn("connect failed: {}", e.toString());
                events.publishLog(id, "ERROR " + describe(e));
                publishTerminal(settled, new ConnectionState(id, ConnectionState.Status.ERROR, null, describe(e)));
            }
        });
    }

    private void publishTerminal(AtomicBoolean settled, ConnectionState s) {
        if (settled.compareAndSet(false, true)) {
            // Drop the per-id slot only if it still points at this
            // attempt (a newer connect may have replaced it).
            attempts.remove(s.connectionId(), settled);
            publish(s);
        }
    }

    /** User-initiated cancel of an in-flight connect. Marks the
     *  current attempt as settled so its eventual CONNECTED/ERROR
     *  publish is suppressed and any late-arriving client is closed
     *  by the connect virtual thread. Publishes DISCONNECTED so UI
     *  state machines clear out of CONNECTING. No-op if no connect
     *  is in flight for this id. */
    public void cancelConnect(String id) {
        AtomicBoolean settled = attempts.remove(id);
        if (settled != null && settled.compareAndSet(false, true)) {
            events.publishLog(id, "connect cancelled");
            publish(ConnectionState.disconnected(id));
        }
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
        // The states map kept its rows for every connection ever
        // observed — fine for a long-running app, but on app exit
        // we want clean state for any test fixture that swaps the
        // ConnectionManager out.
        states.clear();
    }

    /** Drop all in-memory state for {@code id} — connection-level
     *  GC after the user deletes a connection from the store. Without
     *  this, {@link #states} holds a row per ever-seen connection
     *  for the JVM's lifetime. */
    public void forget(String id) {
        disconnect(id);
        states.remove(id);
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
