package com.kubrik.mex.k8s.portforward;

import io.kubernetes.client.openapi.ApiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * v2.8.1 Q2.8-N5 — Multi-backend port-forward opener with
 * ordered-fallback semantics (Decision 5).
 *
 * <p>Decision 5 asks for "Fabric8-first, kubectl fallback". The
 * v2.8.1 technical spec (§3.2) locked the main Kubernetes client
 * to {@code io.kubernetes:client-java} over Fabric8 to avoid
 * a second ~30 MB dependency tree for a single feature — so this
 * implementation delivers the *value* Decision 5 was reaching for
 * (a layered backend chain with graceful degradation) using the
 * openers we can ship today: the client-java {@code PortForward}
 * class as primary, {@link KubectlFallbackOpener} as fallback
 * when the primary trips on WebSocket/SPDY upgrade problems.</p>
 *
 * <p>A true Fabric8-backed primary can be added as an extra
 * delegate in the chain later without touching callers — the
 * opener interface is purposefully small.</p>
 *
 * <p>Fallback trigger: the primary's {@link IOException} is caught
 * and the next delegate tried. If every delegate errors, the
 * aggregate failure is raised with every backend's message chained
 * as a suppressed exception so operators can see exactly why each
 * rung failed.</p>
 */
public final class ChainedPortForwardOpener
        implements PortForwardService.PortForwardOpener {

    private static final Logger log = LoggerFactory.getLogger(ChainedPortForwardOpener.class);

    private final List<PortForwardService.PortForwardOpener> delegates;

    public ChainedPortForwardOpener(List<PortForwardService.PortForwardOpener> delegates) {
        Objects.requireNonNull(delegates, "delegates");
        if (delegates.isEmpty()) {
            throw new IllegalArgumentException("at least one delegate required");
        }
        this.delegates = List.copyOf(delegates);
    }

    public static ChainedPortForwardOpener defaultChain(String kubeconfigPath,
                                                         String contextName) {
        // 3-leg chain — Decision 5 in full:
        //   1. client-java (default; widest auth support).
        //   2. fabric8 (succeeds where client-java's SPDY upgrade
        //      stumbles, no external CLI needed).
        //   3. kubectl (last-resort subprocess for environments
        //      where neither Java path negotiates the upgrade).
        List<PortForwardService.PortForwardOpener> chain = new ArrayList<>();
        chain.add(new DefaultClientJavaOpener());
        chain.add(new Fabric8PortForwardOpener(kubeconfigPath, contextName));
        chain.add(new KubectlFallbackOpener("kubectl", kubeconfigPath, contextName));
        return new ChainedPortForwardOpener(chain);
    }

    @Override
    public StreamPair open(ApiClient client, String namespace, String pod, int remotePort)
            throws IOException {
        IOException first = null;
        List<Throwable> others = new ArrayList<>();
        for (int i = 0; i < delegates.size(); i++) {
            PortForwardService.PortForwardOpener d = delegates.get(i);
            try {
                if (i > 0) {
                    log.info("port-forward falling back to delegate {} ({}) for {}/{}:{}",
                            i, d.getClass().getSimpleName(), namespace, pod, remotePort);
                }
                return d.open(client, namespace, pod, remotePort);
            } catch (IOException ioe) {
                if (first == null) first = ioe;
                else others.add(ioe);
                log.debug("port-forward delegate {} failed: {}",
                        d.getClass().getSimpleName(), ioe.toString());
            }
        }
        IOException out = first == null
                ? new IOException("all port-forward delegates failed")
                : new IOException("port-forward delegates exhausted: " + first.getMessage(), first);
        for (Throwable t : others) out.addSuppressed(t);
        throw out;
    }

    /** Visible for tests — snapshot of the delegate types in order. */
    public List<Class<?>> delegateTypes() {
        List<Class<?>> out = new ArrayList<>(delegates.size());
        for (PortForwardService.PortForwardOpener d : delegates) out.add(d.getClass());
        return List.copyOf(out);
    }

    /** The client-java leg extracted so the chain can name it. */
    public static final class DefaultClientJavaOpener
            implements PortForwardService.PortForwardOpener {
        private final PortForwardService.PortForwardOpener impl;
        public DefaultClientJavaOpener() {
            this.impl = new ClientJavaOpenerShim();
        }

        @Override
        public StreamPair open(ApiClient client, String namespace, String pod, int remotePort)
                throws IOException {
            return impl.open(client, namespace, pod, remotePort);
        }
    }

    /** Thin wrapper around the package-private DefaultPortForwardOpener
     *  in {@link PortForwardService} so the chain can instantiate it
     *  without reflection. */
    static final class ClientJavaOpenerShim
            implements PortForwardService.PortForwardOpener {
        private final PortForwardService.PortForwardOpener production =
                PortForwardService.newDefaultOpener();
        @Override
        public StreamPair open(ApiClient client, String namespace, String pod, int remotePort)
                throws IOException {
            return production.open(client, namespace, pod, remotePort);
        }
    }
}
