package com.kubrik.mex.k8s.portforward;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.LocalPortForward;
import io.kubernetes.client.openapi.ApiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.time.Duration;

/**
 * v2.8.4 — Fabric8-backed port-forward opener (Decision 5 second-leg
 * realisation).
 *
 * <p>Uses Fabric8's {@code podOperations.portForward(...)} which
 * returns a {@link LocalPortForward} bound to a kernel-assigned
 * loopback port. The opener bridges that local port back to the
 * caller via plain TCP — same shape as
 * {@link KubectlFallbackOpener}, so the rest of the
 * {@link PortForwardService} accept loop is unchanged.</p>
 *
 * <p>This leg is plugged into the {@link ChainedPortForwardOpener}
 * between the vanilla client-java leg and the kubectl fallback —
 * Fabric8 typically succeeds where the client-java SPDY upgrade
 * stumbles (older API servers, certain proxy hops) without needing
 * an external CLI.</p>
 *
 * <p>The Fabric8 {@link Config} is derived from the {@code KUBECONFIG}
 * env var when set, otherwise from the user-provided kubeconfig
 * path + context name. The port-forward + the Fabric8 client are
 * both closed when the returned {@link StreamPair#close} is
 * invoked — failing to close either leaks the underlying OkHttp
 * connection pool.</p>
 */
public final class Fabric8PortForwardOpener
        implements PortForwardService.PortForwardOpener {

    private static final Logger log = LoggerFactory.getLogger(Fabric8PortForwardOpener.class);
    private static final Duration READY_WAIT = Duration.ofSeconds(10);

    private final String kubeconfigPath;
    private final String contextName;

    public Fabric8PortForwardOpener(String kubeconfigPath, String contextName) {
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

        Config cfg = buildConfig();
        KubernetesClient kc = new KubernetesClientBuilder().withConfig(cfg).build();
        LocalPortForward forward;
        try {
            forward = kc.pods().inNamespace(namespace).withName(pod)
                    .portForward(remotePort, localForwarderPort);
        } catch (Exception e) {
            try { kc.close(); } catch (Exception ignored) {}
            throw new IOException("fabric8 portForward(" + namespace + "/" + pod
                    + ":" + remotePort + ") failed: " + e.getMessage(), e);
        }

        Socket socket = waitForPortReady(localForwarderPort, READY_WAIT);
        if (socket == null) {
            try { forward.close(); } catch (Exception ignored) {}
            try { kc.close(); } catch (Exception ignored) {}
            throw new IOException("fabric8 port-forward did not become ready within "
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
                try { forward.close(); } catch (Exception ignored) {}
                try { kc.close(); } catch (Exception ignored) {}
                log.debug("fabric8 fallback closed pod={} :{} → :{}",
                        pod, remotePort, localForwarderPort);
            }
        };
    }

    private Config buildConfig() {
        if (kubeconfigPath != null && !kubeconfigPath.isBlank()) {
            try {
                String content = java.nio.file.Files.readString(Path.of(kubeconfigPath));
                return Config.fromKubeconfig(contextName, content, kubeconfigPath);
            } catch (Exception ignored) {
                // Fall through — let the builder pick up KUBECONFIG / ~/.kube/config.
            }
        }
        return new ConfigBuilder().build();
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
