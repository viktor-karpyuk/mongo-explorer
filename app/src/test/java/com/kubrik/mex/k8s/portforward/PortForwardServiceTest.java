package com.kubrik.mex.k8s.portforward;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.cluster.KubeClusterDao;
import com.kubrik.mex.k8s.events.PortForwardEvent;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.model.PortForwardSession;
import com.kubrik.mex.k8s.model.PortForwardTarget;
import com.kubrik.mex.store.Database;
import io.kubernetes.client.openapi.ApiClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class PortForwardServiceTest {

    @TempDir Path dataDir;

    private Database db;
    private PortForwardAuditDao dao;
    private long clusterId;
    private EventBus bus;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        dao = new PortForwardAuditDao(db);
        // portforward_audit FKs k8s_clusters; seed a row so the open
        // writes don't bounce on the FK constraint.
        clusterId = new KubeClusterDao(db).insert(
                "test", "/kube", "ctx", Optional.empty(), Optional.empty());
        bus = new EventBus();
    }

    @AfterEach
    void tearDown() throws Exception { if (db != null) db.close(); }

    @Test
    void open_pod_target_writes_audit_and_publishes_opened_event() throws Exception {
        CapturingOpener opener = new CapturingOpener();
        TestClientFactory factory = new TestClientFactory();
        PortForwardService svc = new PortForwardService(factory, dao, bus, opener);

        List<PortForwardEvent> events = new CopyOnWriteArrayList<>();
        bus.onPortForward(events::add);

        PortForwardSession s = svc.open(ref(clusterId), "conn",
                PortForwardTarget.forPod("mongo", "mongo-0", 27017));

        assertEquals(1, svc.openSessionIds().size());
        assertTrue(s.localPort() > 0);
        assertTrue(events.stream().anyMatch(e -> e instanceof PortForwardEvent.Opened));
        assertEquals(1, dao.countOpen("conn"));

        svc.close(s.auditRowId(), "TEST");
        assertEquals(0, svc.openSessionIds().size());
        assertEquals(0, dao.countOpen("conn"));
        assertTrue(events.stream().anyMatch(e -> e instanceof PortForwardEvent.Closed));

        try (ResultSet rs = db.connection().createStatement()
                .executeQuery("SELECT reason_closed FROM portforward_audit WHERE id = "
                        + s.auditRowId())) {
            assertTrue(rs.next());
            assertEquals("TEST", rs.getString("reason_closed"));
        }

        svc.close();
    }

    @Test
    void close_is_idempotent() throws Exception {
        PortForwardService svc = new PortForwardService(
                new TestClientFactory(), dao, bus, new CapturingOpener());
        PortForwardSession s = svc.open(ref(clusterId), "conn",
                PortForwardTarget.forPod("mongo", "p", 27017));
        svc.close(s.auditRowId());
        svc.close(s.auditRowId());  // must not throw or double-emit
        svc.close(99_999L);         // nonexistent id must be a no-op
        svc.close();
    }

    @Test
    void close_all_tears_every_session_down() throws Exception {
        PortForwardService svc = new PortForwardService(
                new TestClientFactory(), dao, bus, new CapturingOpener());
        svc.open(ref(clusterId), "c1", PortForwardTarget.forPod("mongo", "p1", 27017));
        svc.open(ref(clusterId), "c2", PortForwardTarget.forPod("mongo", "p2", 27017));
        assertEquals(2, svc.openSessionIds().size());
        svc.closeAll();
        assertEquals(0, svc.openSessionIds().size());
    }

    @Test
    void pipes_bytes_from_client_socket_through_stub_streams() throws Exception {
        ByteArrayOutputStream upstreamCaptor = new ByteArrayOutputStream();
        ByteArrayInputStream downstream = new ByteArrayInputStream(
                "hello from pod".getBytes(StandardCharsets.UTF_8));
        CapturingOpener opener = new CapturingOpener(downstream, upstreamCaptor);

        PortForwardService svc = new PortForwardService(
                new TestClientFactory(), dao, bus, opener);
        PortForwardSession s = svc.open(ref(clusterId), "conn",
                PortForwardTarget.forPod("mongo", "p", 27017));

        try (Socket client = new Socket(InetAddress.getLoopbackAddress(), s.localPort())) {
            client.getOutputStream().write("ping".getBytes(StandardCharsets.UTF_8));
            client.getOutputStream().flush();
            // Read what the pod "sent" back.
            byte[] buf = new byte[64];
            int n = client.getInputStream().read(buf);
            assertTrue(n > 0, "service should forward pod bytes back to client");
        }
        // Give the upstream pump a moment to drain, then verify capture.
        Thread.sleep(100);
        String sent = upstreamCaptor.toString(StandardCharsets.UTF_8);
        assertTrue(sent.contains("ping"),
                "upstream should receive bytes the client wrote; got: " + sent);
        svc.close();
    }

    /* ============================ fixtures ============================ */

    private static K8sClusterRef ref(long id) {
        return new K8sClusterRef(id, "t", "/k", "ctx",
                Optional.empty(), Optional.empty(), 0L, Optional.empty());
    }

    /** Stub that skips the real client build and never talks to the API. */
    static final class TestClientFactory extends KubeClientFactory {
        @Override public ApiClient get(K8sClusterRef ref) { return null; }
        @Override public ApiClient fresh(K8sClusterRef ref) { return null; }
    }

    /**
     * Stub opener: records that open() was called and hands back the
     * test's in-memory streams. Since the opener is invoked on every
     * accepted client connection, each call constructs a fresh pair
     * (the downstream InputStream is exhausted once).
     */
    static final class CapturingOpener implements PortForwardService.PortForwardOpener {
        final List<String> calls = new CopyOnWriteArrayList<>();
        private final InputStream downstream;
        private final OutputStream upstream;

        CapturingOpener() {
            this(new ByteArrayInputStream(new byte[0]), new ByteArrayOutputStream());
        }

        CapturingOpener(InputStream downstream, OutputStream upstream) {
            this.downstream = downstream;
            this.upstream = upstream;
        }

        @Override
        public StreamPair open(ApiClient client, String namespace, String pod, int remotePort) {
            calls.add(namespace + "/" + pod + ":" + remotePort);
            return new StreamPair() {
                @Override public InputStream downstream() { return downstream; }
                @Override public OutputStream upstream() { return upstream; }
                @Override public void close() {
                    try { upstream.close(); } catch (IOException ignored) {}
                    try { downstream.close(); } catch (IOException ignored) {}
                }
            };
        }
    }
}
