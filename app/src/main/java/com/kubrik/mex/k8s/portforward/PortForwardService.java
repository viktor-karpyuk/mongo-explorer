package com.kubrik.mex.k8s.portforward;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.events.PortForwardEvent;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.model.PortForwardSession;
import com.kubrik.mex.k8s.model.PortForwardTarget;
import io.kubernetes.client.PortForward;
import io.kubernetes.client.openapi.ApiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * v2.8.1 Q2.8.1-C3 — The top-level port-forward service.
 *
 * <p>For each forward:</p>
 * <ol>
 *   <li>Resolve the target's backing pod (Service → Endpoints
 *       lookup or explicit pod name).</li>
 *   <li>Bind a {@link ServerSocket} on a kernel-assigned ephemeral
 *       port on 127.0.0.1.</li>
 *   <li>Spawn a virtual thread as the accept loop. Every accepted
 *       client connection spins off a pair of pipe threads
 *       (down-stream + up-stream), using the Kubernetes client-java
 *       {@link PortForward} primitive over a WebSocket SPDY
 *       upgrade to the API server.</li>
 *   <li>Write an {@code portforward_audit} row on open; flip
 *       {@code closed_at} on close. Publish {@link PortForwardEvent}
 *       at each milestone.</li>
 * </ol>
 *
 * <p>Close is idempotent: {@link #close(long)} safely handles
 * already-closed sessions. The service holds a weak reference to
 * every open {@link ServerSocket} and exposes {@link #closeAll} for
 * the JVM-shutdown hook and app teardown.</p>
 *
 * <p>This chunk lands the wiring; live streaming against a kind
 * cluster is Q2.8.1-L. For unit tests we inject a
 * {@link PortForwardOpener} seam so the control flow can be driven
 * with a stub that doesn't require a WebSocket.</p>
 */
public final class PortForwardService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PortForwardService.class);

    private final KubeClientFactory clientFactory;
    private final PortForwardAuditDao auditDao;
    private final EventBus events;
    private final PortForwardOpener opener;

    private final ConcurrentMap<Long, Handle> handlesByAuditId = new ConcurrentHashMap<>();

    public PortForwardService(KubeClientFactory clientFactory,
                               PortForwardAuditDao auditDao,
                               EventBus events) {
        this(clientFactory, auditDao, events, new DefaultPortForwardOpener());
    }

    PortForwardService(KubeClientFactory clientFactory,
                        PortForwardAuditDao auditDao,
                        EventBus events,
                        PortForwardOpener opener) {
        this.clientFactory = Objects.requireNonNull(clientFactory, "clientFactory");
        this.auditDao = Objects.requireNonNull(auditDao, "auditDao");
        this.events = Objects.requireNonNull(events, "events");
        this.opener = Objects.requireNonNull(opener, "opener");
    }

    /**
     * Open a new forward. Blocks only long enough to bind the local
     * listener + write the audit row; the accept loop runs on a
     * virtual thread.
     */
    public PortForwardSession open(K8sClusterRef clusterRef,
                                     String connectionId,
                                     PortForwardTarget target) throws IOException {
        Objects.requireNonNull(clusterRef, "clusterRef");
        Objects.requireNonNull(connectionId, "connectionId");
        target.validate();

        ApiClient client = clientFactory.get(clusterRef);
        // Resolve the backing pod eagerly so we fail fast if the
        // Service has no ready endpoint — saves binding a local
        // listener that we'd have to immediately close. Pod targets
        // skip the API roundtrip entirely.
        String podName;
        if (target.pod().isPresent()) {
            podName = target.pod().get();
        } else {
            try {
                podName = new PodResolver(client).resolvePod(target);
            } catch (Exception e) {
                throw new IOException("resolve pod for " + target.namespace()
                        + "/" + target.name() + ": " + e.getMessage(), e);
            }
        }

        ServerSocket listener = EphemeralPortAllocator.reserveLoopback();
        long openedAt = System.currentTimeMillis();
        long auditRowId;
        try {
            auditRowId = auditDao.insertOpen(connectionId, clusterRef.id(),
                    target, listener.getLocalPort(), openedAt);
        } catch (Exception e) {
            try { listener.close(); } catch (IOException ignored) {}
            throw new IOException("audit insert failed: " + e.getMessage(), e);
        }

        PortForwardSession session = new PortForwardSession(
                clusterRef.id(), connectionId, target,
                listener.getLocalPort(), openedAt, auditRowId);

        AtomicBoolean closing = new AtomicBoolean(false);
        Handle handle = new Handle(session, listener, closing);
        handlesByAuditId.put(auditRowId, handle);

        Thread acceptLoop = Thread.ofVirtual()
                .name("k8s-pfwd-accept-" + auditRowId)
                .start(() -> runAcceptLoop(handle, client, target, podName));
        handle.acceptLoop = acceptLoop;

        events.publishPortForward(new PortForwardEvent.Opened(connectionId,
                auditRowId, listener.getLocalPort(), openedAt));
        log.info("port-forward open id={} {} {}/{}:{} → 127.0.0.1:{}",
                auditRowId, target.kindLabel(), target.namespace(), target.name(),
                target.remotePort(), listener.getLocalPort());
        return session;
    }

    public void close(long auditRowId) {
        close(auditRowId, "MANUAL");
    }

    public void close(long auditRowId, String reason) {
        Handle h = handlesByAuditId.remove(auditRowId);
        if (h == null) return;
        if (!h.closing.compareAndSet(false, true)) return;
        try { h.listener.close(); } catch (IOException ignored) {}
        try {
            auditDao.markClosed(auditRowId, System.currentTimeMillis(), reason);
        } catch (Exception e) {
            log.debug("audit close {} failed: {}", auditRowId, e.toString());
        }
        events.publishPortForward(new PortForwardEvent.Closed(
                h.session.connectionId(), auditRowId, reason,
                System.currentTimeMillis()));
    }

    public void closeAll() {
        List<Long> ids = List.copyOf(handlesByAuditId.keySet());
        for (Long id : ids) close(id, "APP_EXIT");
    }

    @Override
    public void close() { closeAll(); }

    /** Exposed for tests — snapshot of the open audit ids. */
    public java.util.Set<Long> openSessionIds() {
        return java.util.Set.copyOf(handlesByAuditId.keySet());
    }

    /* ======================== accept loop + pumps ======================== */

    private void runAcceptLoop(Handle handle, ApiClient client,
                                 PortForwardTarget target, String podName) {
        ServerSocket listener = handle.listener;
        while (!handle.closing.get() && !listener.isClosed()) {
            Socket client1;
            try {
                client1 = listener.accept();
            } catch (SocketException se) {
                // Expected when close() shuts the listener down.
                return;
            } catch (IOException ioe) {
                log.debug("accept loop {} failed: {}", handle.session.auditRowId(), ioe.toString());
                return;
            }
            Thread.ofVirtual()
                    .name("k8s-pfwd-conn-" + handle.session.auditRowId())
                    .start(() -> handleClientConnection(handle, client, target, podName, client1));
        }
    }

    private void handleClientConnection(Handle handle, ApiClient client,
                                          PortForwardTarget target, String podName,
                                          Socket clientSocket) {
        long auditRowId = handle.session.auditRowId();
        PortForwardOpener.StreamPair streams = null;
        try {
            streams = opener.open(client, target.namespace(), podName, target.remotePort());
            InputStream pfDown = streams.downstream();   // data from pod
            OutputStream pfUp = streams.upstream();       // data to pod
            InputStream clientIn = clientSocket.getInputStream();
            OutputStream clientOut = clientSocket.getOutputStream();

            Thread upPump = Thread.ofVirtual()
                    .name("k8s-pfwd-up-" + auditRowId)
                    .start(() -> pipe(clientIn, pfUp));
            pipe(pfDown, clientOut);
            // Interrupt is a no-op against blocking socket reads, so
            // don't pretend it does anything. The finally block below
            // closes clientSocket which unblocks the up-pump's read.
        } catch (IOException ioe) {
            log.debug("pipe {} errored: {}", auditRowId, ioe.toString());
            events.publishPortForward(new PortForwardEvent.Error(
                    handle.session.connectionId(), auditRowId,
                    ioe.getMessage() == null ? ioe.getClass().getSimpleName() : ioe.getMessage(),
                    System.currentTimeMillis()));
        } catch (Exception e) {
            log.debug("pipe {} errored: {}", auditRowId, e.toString());
            events.publishPortForward(new PortForwardEvent.Error(
                    handle.session.connectionId(), auditRowId,
                    e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage(),
                    System.currentTimeMillis()));
        } finally {
            try { clientSocket.close(); } catch (IOException ignored) {}
            if (streams != null) {
                try { streams.close(); } catch (Exception ignored) {}
            }
        }
    }

    /** Blocking byte-for-byte copy. Returns on EOF or IO error. */
    private static void pipe(InputStream in, OutputStream out) {
        byte[] buf = new byte[8 * 1024];
        try {
            int n;
            while ((n = in.read(buf)) != -1) {
                out.write(buf, 0, n);
                out.flush();
            }
        } catch (IOException ignored) {
            // peer closed — the caller teardown handles it.
        }
    }

    /* ============================ types ============================ */

    private static final class Handle {
        final PortForwardSession session;
        final ServerSocket listener;
        final AtomicBoolean closing;
        volatile Thread acceptLoop;
        Handle(PortForwardSession session, ServerSocket listener, AtomicBoolean closing) {
            this.session = session;
            this.listener = listener;
            this.closing = closing;
        }
    }

    /**
     * Seam for the streaming bit. The production implementation
     * wraps {@link PortForward}; tests swap in a stub that returns
     * pre-canned streams so the service's control flow is verifiable
     * without a live cluster.
     */
    public interface PortForwardOpener {
        StreamPair open(ApiClient client, String namespace, String pod, int remotePort)
                throws IOException;

        interface StreamPair extends AutoCloseable {
            InputStream downstream();
            OutputStream upstream();
            @Override void close();
        }
    }

    private static final class DefaultPortForwardOpener implements PortForwardOpener {
        @Override
        public StreamPair open(ApiClient client, String namespace, String pod, int remotePort)
                throws IOException {
            try {
                PortForward pf = new PortForward(client);
                PortForward.PortForwardResult result = pf.forward(
                        namespace, pod, List.of(remotePort));
                // Eagerly resolve both streams so close() can propagate
                // to their underlying WebSocketStreamHandler — a no-op
                // close() leaks the SPDY upgrade connection for the
                // life of the JVM.
                InputStream downstream;
                try { downstream = result.getInputStream(remotePort); }
                catch (IOException ioe) {
                    throw new IOException("resolve pfwd input stream: " + ioe.getMessage(), ioe);
                }
                OutputStream upstream = result.getOutboundStream(remotePort);
                return new StreamPair() {
                    @Override public InputStream downstream() { return downstream; }
                    @Override public OutputStream upstream() { return upstream; }
                    @Override public void close() {
                        // Closing either stream cascades to the
                        // WebSocketStreamHandler and closes the SPDY
                        // upgrade. We close both for robustness.
                        try { downstream.close(); } catch (IOException ignored) {}
                        try { upstream.close(); } catch (IOException ignored) {}
                    }
                };
            } catch (io.kubernetes.client.openapi.ApiException ae) {
                throw new IOException("port-forward API failed: "
                        + ae.getCode() + " " + ae.getMessage(), ae);
            }
        }
    }

}
