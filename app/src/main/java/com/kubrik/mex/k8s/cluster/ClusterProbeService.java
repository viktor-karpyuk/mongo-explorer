package com.kubrik.mex.k8s.cluster;

import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.model.ClusterProbeResult;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.VersionApi;
import io.kubernetes.client.openapi.models.V1NodeList;
import io.kubernetes.client.openapi.models.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * v2.8.1 Q2.8.1-A3 — Single-shot "is this cluster reachable?" check.
 *
 * <p>Runs {@code /version} against the target ApiClient and, opportunistically,
 * {@code /api/v1/nodes} to report a node count. Bounded by a short
 * budget (default 8 s — see milestone risk register entry for
 * exec-plugin latency) and classifies failures into the {@link
 * ClusterProbeResult.Status} vocabulary the UI needs to render a
 * meaningful chip.</p>
 *
 * <p>The caller is expected to call {@link #close()} when the service
 * is discarded (e.g. ClustersPane close). We own a small fixed thread
 * pool rather than virtual threads because each probe blocks on a
 * single OkHttp call and we want a bounded cap.</p>
 */
public class ClusterProbeService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClusterProbeService.class);

    /** How long a single probe is allowed to take end-to-end. */
    public static final long DEFAULT_BUDGET_MS = 8_000L;

    private final KubeClientFactory clientFactory;
    private final ExecutorService exec;
    private final long budgetMs;

    public ClusterProbeService(KubeClientFactory clientFactory) {
        this(clientFactory, DEFAULT_BUDGET_MS);
    }

    public ClusterProbeService(KubeClientFactory clientFactory, long budgetMs) {
        this.clientFactory = clientFactory;
        this.budgetMs = budgetMs;
        this.exec = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "k8s-probe");
            t.setDaemon(true);
            return t;
        });
    }

    /** Block until the probe completes (or the budget expires). */
    public ClusterProbeResult probe(K8sClusterRef ref) {
        long at = System.currentTimeMillis();
        try {
            // Use the factory's cached client — a fresh() client per probe
            // leaks OkHttp dispatcher threads + a connection pool every
            // call, and mutating setReadTimeout on a shared client is
            // forbidden by the factory's contract. callWithBudget()
            // enforces the budget independently of the client's own read
            // timeout.
            ApiClient client = clientFactory.get(ref);
            VersionInfo info = callWithBudget(() -> new VersionApi(client).getCode().execute());
            String version = info == null ? null : info.getGitVersion();

            Optional<Integer> nodes = Optional.empty();
            try {
                V1NodeList nodeList = callWithBudget(() ->
                        new CoreV1Api(client).listNode().execute());
                nodes = Optional.of(nodeList.getItems().size());
            } catch (ApiException denied) {
                // Reachable but no RBAC to list nodes — still "REACHABLE"
                // per the spec. Keep nodes empty.
                log.debug("nodes list denied on {}: {}", ref.coordinates(), denied.getCode());
            } catch (Exception ignored) {
                // Best-effort — absence of node count doesn't fail the probe.
            }

            return new ClusterProbeResult(
                    ClusterProbeResult.Status.REACHABLE,
                    Optional.ofNullable(version),
                    nodes,
                    Optional.empty(),
                    at);
        } catch (TimeoutException te) {
            return fail(ClusterProbeResult.Status.TIMED_OUT,
                    "probe exceeded " + budgetMs + " ms budget", at);
        } catch (ApiException ae) {
            int code = ae.getCode();
            if (code == 401 || code == 403) {
                return fail(ClusterProbeResult.Status.AUTH_FAILED,
                        ae.getMessage() == null ? "HTTP " + code : ae.getMessage(), at);
            }
            return fail(ClusterProbeResult.Status.UNREACHABLE,
                    "HTTP " + code + (ae.getMessage() == null ? "" : ": " + ae.getMessage()),
                    at);
        } catch (IOException ioe) {
            // KubeClientFactory.fresh bubbles up as IOException when
            // the kubeconfig / context is missing.
            return fail(ClusterProbeResult.Status.UNREACHABLE, ioe.getMessage(), at);
        } catch (Exception e) {
            Throwable root = rootCause(e);
            String msg = root.getMessage() == null ? root.getClass().getSimpleName() : root.getMessage();
            if (isPluginMissing(root)) {
                return fail(ClusterProbeResult.Status.PLUGIN_MISSING,
                        extractBinary(msg), at);
            }
            return fail(ClusterProbeResult.Status.UNREACHABLE, msg, at);
        }
    }

    @Override
    public void close() {
        exec.shutdownNow();
    }

    private <T> T callWithBudget(Callable<T> task) throws Exception {
        Future<T> f = exec.submit(task);
        try {
            return f.get(budgetMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof Exception xe) throw xe;
            throw new RuntimeException(cause);
        } catch (TimeoutException te) {
            f.cancel(true);
            throw te;
        }
    }

    private static ClusterProbeResult fail(ClusterProbeResult.Status s, String msg, long at) {
        return new ClusterProbeResult(s,
                Optional.empty(),
                Optional.empty(),
                Optional.ofNullable(msg),
                at);
    }

    private static Throwable rootCause(Throwable t) {
        Throwable cur = t;
        while (cur.getCause() != null && cur.getCause() != cur) cur = cur.getCause();
        return cur;
    }

    private static boolean isPluginMissing(Throwable t) {
        if (t == null) return false;
        String cn = t.getClass().getName();
        if (cn.endsWith("FileNotFoundException") || cn.endsWith("NoSuchFileException")) {
            return true;
        }
        String m = t.getMessage();
        return m != null && (m.contains("not found") || m.contains("No such file"))
                && (m.contains("aws-iam-authenticator") || m.contains("gke-gcloud-auth-plugin")
                        || m.contains("exec plugin") || m.contains("exec:"));
    }

    /**
     * Names of the exec plugins we recognise. Order matters only for
     * readability — we match the first that appears anywhere in the
     * message. Mirrors {@link #isPluginMissing} so a positive detection
     * reliably extracts the right name.
     */
    private static final String[] KNOWN_PLUGINS = {
            "aws-iam-authenticator",
            "gke-gcloud-auth-plugin",
            "aws-vault",
            "kubelogin",
            "kubectl-oidc_login"
    };

    private static String extractBinary(String msg) {
        if (msg == null) return "exec plugin missing";
        for (String plugin : KNOWN_PLUGINS) {
            if (msg.contains(plugin)) return "exec plugin missing: " + plugin;
        }
        // Fallback — return the full message so the UI shows the original
        // system error rather than a misleading substring.
        return "exec plugin missing: " + msg;
    }
}
