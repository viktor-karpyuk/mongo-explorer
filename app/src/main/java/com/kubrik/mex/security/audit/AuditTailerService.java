package com.kubrik.mex.security.audit;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.model.ConnectionState;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * v2.6 Q2.6-C follow-up — per-connection audit-log tailer lifecycle.
 * Subscribes to {@link EventBus#onState} transitions; on CONNECTED the
 * service probes {@code getCmdLineOpts} for an {@code auditLog.path}
 * the local filesystem can read, and if found spawns an
 * {@link AuditLogTailer} that pipes every parsed event into the
 * {@link AuditIndex}. On DISCONNECTED / ERROR the tailer is closed.
 *
 * <p>No {@code MongoConnection} schema change: the audit-log path is
 * authoritative on the server, so we read it from there. If Mongo
 * isn't configured with {@code auditLog.destination = file}, or the
 * path isn't readable from this host (common for containerised
 * clusters), the tailer stays dormant — the Audit pane's empty-state
 * copy already explains this case.</p>
 */
public final class AuditTailerService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AuditTailerService.class);

    private final ConnectionManager manager;
    private final AuditIndex index;
    private final EventBus.Subscription stateSubscription;
    private final ConcurrentMap<String, AuditLogTailer> tailers = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    public AuditTailerService(ConnectionManager manager, EventBus events, AuditIndex index) {
        this.manager = manager;
        this.index = index;
        this.stateSubscription = events.onState(this::onState);
    }

    /** Visible for tests and for the app's CLI / headless entry to poke
     *  one connection manually. Returns true if a tailer started; false
     *  if the probe found nothing readable.
     *
     *  <p>Synchronised to serialise the close-old-then-start-new sequence
     *  against rapid CONNECTED → DISCONNECTED → CONNECTED bursts (user
     *  flicking the network, flaky cluster, etc.). Without the monitor,
     *  two onState handlers firing in parallel could leave a stray
     *  tailer running against the same file handle. */
    public synchronized boolean startFor(String connectionId) {
        MongoService svc = manager.service(connectionId);
        if (svc == null) return false;
        Path auditPath = probeAuditLogPath(svc);
        if (auditPath == null) {
            log.debug("audit tailer {}: no readable auditLog.path", connectionId);
            return false;
        }
        AuditLogTailer existing = tailers.remove(connectionId);
        if (existing != null) existing.close();
        AuditLogTailer tailer = new AuditLogTailer(auditPath,
                event -> index.insert(connectionId, event));
        tailers.put(connectionId, tailer);
        tailer.start();
        log.info("audit tailer started for {} on {}", connectionId, auditPath);
        return true;
    }

    public synchronized void stopFor(String connectionId) {
        AuditLogTailer tailer = tailers.remove(connectionId);
        if (tailer == null) return;
        try { tailer.close(); }
        catch (Exception ignored) {}
        log.debug("audit tailer stopped for {}", connectionId);
    }

    @Override
    public void close() {
        closed = true;
        try { stateSubscription.close(); }
        catch (Exception ignored) {}
        for (Map.Entry<String, AuditLogTailer> e : tailers.entrySet()) {
            try { e.getValue().close(); }
            catch (Exception ignored) {}
        }
        tailers.clear();
    }

    /* =========================== state glue =========================== */

    private void onState(ConnectionState s) {
        if (closed) return;
        switch (s.status()) {
            case CONNECTED -> Thread.startVirtualThread(() -> {
                try { startFor(s.connectionId()); }
                catch (Exception e) {
                    log.debug("audit tailer start failed for {}: {}",
                            s.connectionId(), e.getMessage());
                }
            });
            case DISCONNECTED, ERROR -> stopFor(s.connectionId());
            default -> { /* CONNECTING: wait for the terminal state */ }
        }
    }

    /* ============================ probe ============================ */

    /** Reads {@code getCmdLineOpts} off the live service and hands the
     *  reply to {@link #resolveAuditPath} for the decidable classification.
     *  Returns {@code null} if the command fails. */
    static Path probeAuditLogPath(MongoService svc) {
        try {
            Document reply = svc.database("admin").runCommand(new Document("getCmdLineOpts", 1));
            return resolveAuditPath(reply);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Pure-logic slice of the probe: given a pre-fetched
     * {@code getCmdLineOpts} reply, returns the audit log {@link Path}
     * when:
     * <ul>
     *   <li>{@code parsed.auditLog} exists in the reply,</li>
     *   <li>{@code auditLog.destination} is {@code "file"} or absent
     *       (absent = server default),</li>
     *   <li>{@code auditLog.path} is set and the path is readable from
     *       the local filesystem (Mongo in a container gets filtered
     *       out here).</li>
     * </ul>
     * Returns {@code null} otherwise. Exposed package-private so tests
     * can exercise every decision branch without a live server.
     */
    static Path resolveAuditPath(Document reply) {
        if (reply == null) return null;
        Document parsed = reply.get("parsed", Document.class);
        if (parsed == null) return null;
        Document auditLog = parsed.get("auditLog", Document.class);
        if (auditLog == null) return null;
        String destination = auditLog.getString("destination");
        if (destination != null && !destination.equalsIgnoreCase("file")) return null;
        String pathStr = auditLog.getString("path");
        if (pathStr == null || pathStr.isBlank()) return null;
        // Normalise before the readability check so a malicious server
        // config that injects '../../../etc/passwd' collapses to its
        // canonical form; Files.isReadable then checks the resolved
        // file, not the relative traversal path. The server-side config
        // is technically trusted, but collapsing the path is cheap
        // defence-in-depth.
        Path path;
        try {
            path = Path.of(pathStr).toAbsolutePath().normalize();
        } catch (Exception bad) {
            return null;
        }
        if (!Files.isReadable(path)) return null;
        return path;
    }
}
