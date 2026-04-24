package com.kubrik.mex.k8s.portforward;

import io.kubernetes.client.openapi.ApiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * v2.8.1 Q2.8-N — "kubectl fallback" port-forward opener (Decision 5).
 *
 * <p>The default {@link PortForwardService.PortForwardOpener}
 * (the vanilla client-java {@code PortForward} class) fails to
 * establish a tunnel in a few well-known situations:</p>
 * <ul>
 *   <li>API servers that route SPDY over a proxy that strips the
 *       WebSocket {@code Upgrade} header (common with Cloudflare
 *       and some corporate egress proxies).</li>
 *   <li>Older client-go server versions that exec credential
 *       plugins the Java client doesn't implement (AWS EKS
 *       {@code aws-iam-authenticator}, GKE gcloud auth plugin).</li>
 *   <li>Plain bugs in the WebSocket → SPDY bridge that surface
 *       as an immediate stream close on first write.</li>
 * </ul>
 *
 * <p>This opener shells out to {@code kubectl port-forward} on a
 * second ephemeral loopback port, then opens a plain TCP socket
 * to that port and returns the socket streams — so the rest of
 * {@link PortForwardService}'s accept loop / byte pump is
 * unchanged. Each open spawns a supervised subprocess; close()
 * kills the subprocess and the TCP socket together.</p>
 *
 * <p>Requires {@code kubectl} on {@code PATH} and a kubeconfig
 * reachable to it (either the process's {@code ~/.kube/config}
 * or the {@code KUBECONFIG} env var). Wire this opener via the
 * test-seam ctor when the milestone decides to flip the default:
 * the production service currently defaults to the client-java
 * opener.</p>
 */
public final class KubectlFallbackOpener implements PortForwardService.PortForwardOpener {

    private static final Logger log = LoggerFactory.getLogger(KubectlFallbackOpener.class);
    private static final Duration READY_WAIT = Duration.ofSeconds(10);

    private final String kubectlPath;
    private final String kubeconfigPath;
    private final String contextName;

    public KubectlFallbackOpener(String kubectlPath, String kubeconfigPath, String contextName) {
        this.kubectlPath = kubectlPath == null ? "kubectl" : kubectlPath;
        this.kubeconfigPath = kubeconfigPath;
        this.contextName = contextName;
    }

    @Override
    public StreamPair open(ApiClient client, String namespace, String pod, int remotePort)
            throws IOException {
        int localForwarderPort;
        try (ServerSocket probe = new ServerSocket(0)) {
            probe.setReuseAddress(true);
            localForwarderPort = probe.getLocalPort();
        }

        List<String> argv = new ArrayList<>();
        argv.add(kubectlPath);
        if (kubeconfigPath != null && !kubeconfigPath.isBlank()) {
            argv.add("--kubeconfig"); argv.add(kubeconfigPath);
        }
        if (contextName != null && !contextName.isBlank()) {
            argv.add("--context"); argv.add(contextName);
        }
        argv.add("-n"); argv.add(namespace);
        argv.add("port-forward");
        argv.add("--address"); argv.add("127.0.0.1");
        argv.add("pod/" + pod);
        argv.add(localForwarderPort + ":" + remotePort);

        ProcessBuilder pb = new ProcessBuilder(argv).redirectErrorStream(true);
        Process proc = pb.start();
        Thread drain = new Thread(() -> {
            try (var in = proc.getInputStream()) {
                byte[] buf = new byte[1024];
                while (in.read(buf) != -1) { /* discard */ }
            } catch (IOException ignored) {}
        }, "kubectl-pfwd-drain");
        drain.setDaemon(true);
        drain.start();

        Socket socket = waitForPortReady(localForwarderPort, READY_WAIT);
        if (socket == null) {
            proc.destroyForcibly();
            throw new IOException("kubectl port-forward did not become ready within "
                    + READY_WAIT.toSeconds() + "s (pod=" + pod
                    + " :" + remotePort + " → :" + localForwarderPort + ")");
        }

        InputStream downstream = socket.getInputStream();
        OutputStream upstream = socket.getOutputStream();
        return new StreamPair() {
            @Override public InputStream downstream() { return downstream; }
            @Override public OutputStream upstream()   { return upstream; }
            @Override public void close() {
                try { socket.close(); } catch (IOException ignored) {}
                proc.destroy();
                try {
                    if (!proc.waitFor(5, TimeUnit.SECONDS)) {
                        proc.destroyForcibly();
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    proc.destroyForcibly();
                }
                log.debug("kubectl fallback closed pod={} :{} → :{}",
                        pod, remotePort, localForwarderPort);
            }
        };
    }

    private static Socket waitForPortReady(int port, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try {
                Socket s = new Socket();
                s.connect(new InetSocketAddress("127.0.0.1", port), 500);
                return s;
            } catch (IOException retry) {
                try { Thread.sleep(200); }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
        }
        return null;
    }
}
