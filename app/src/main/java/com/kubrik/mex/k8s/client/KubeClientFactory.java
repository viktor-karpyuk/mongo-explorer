package com.kubrik.mex.k8s.client;

import com.kubrik.mex.k8s.model.K8sClusterRef;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * v2.8.1 Q2.8.1-A2 — Builds (and caches) {@link ApiClient} instances
 * from Mongo-Explorer-persisted {@link K8sClusterRef}s.
 *
 * <p>The official {@code io.kubernetes:client-java} library already
 * knows how to parse a kubeconfig and wire up every auth provider we
 * care about (exec plugins, OIDC, static tokens, client-cert,
 * in-cluster). We delegate that work to {@link KubeConfig#loadKubeConfig}
 * + {@link ClientBuilder#kubeconfig}; this factory only adds two
 * things:</p>
 *
 * <ul>
 *   <li><b>Caching.</b> One {@code ApiClient} per ref — rebuilt only
 *       when the kubeconfig file's mtime changes. Building a fresh
 *       client for every API call would create thousands of OkHttp
 *       pools and break informer longevity.</li>
 *   <li><b>Context selection.</b> A ref points at one named context;
 *       we call {@link KubeConfig#setContext} before handing the
 *       config to {@code ClientBuilder}.</li>
 * </ul>
 *
 * <p>Cache invalidation is coarse but correct: the next
 * {@link #get(K8sClusterRef)} after an mtime change rebuilds the
 * client. We intentionally do <i>not</i> cache across JVM restarts —
 * exec-plugin credentials have short TTLs and the next boot should
 * re-probe fresh.</p>
 */
public final class KubeClientFactory {

    private static final Logger log = LoggerFactory.getLogger(KubeClientFactory.class);

    /** Read timeout for routine REST calls — matches the probe budget upstream. */
    public static final int DEFAULT_READ_TIMEOUT_MS = 15_000;

    private final ConcurrentMap<CacheKey, Cached> cache = new ConcurrentHashMap<>();

    public KubeClientFactory() {}

    /**
     * Return an {@link ApiClient} targeting the ref's context. Clients
     * are shared across callers for a given ref; do not call
     * {@link ApiClient#setBasePath} or otherwise mutate the cached
     * instance — use {@link #fresh(K8sClusterRef)} if you need a
     * disposable.
     */
    public ApiClient get(K8sClusterRef ref) throws IOException {
        Objects.requireNonNull(ref, "ref");
        CacheKey key = new CacheKey(ref.kubeconfigPath(), ref.contextName());
        long mtime = safeMtime(ref.kubeconfigPath());
        Cached existing = cache.get(key);
        if (existing != null && existing.mtime == mtime) {
            return existing.client;
        }
        ApiClient client = build(ref);
        cache.put(key, new Cached(client, mtime));
        return client;
    }

    /**
     * Build a one-shot client without touching the cache. Used when a
     * caller needs a short-lived connection with custom timeouts
     * (e.g. the {@code ClusterProbeService} which uses an aggressive
     * 5 s /version probe).
     */
    public ApiClient fresh(K8sClusterRef ref) throws IOException {
        return build(ref);
    }

    /** Evict the cached client for a ref — used after "refresh" UI actions. */
    public void invalidate(K8sClusterRef ref) {
        cache.remove(new CacheKey(ref.kubeconfigPath(), ref.contextName()));
    }

    public void invalidateAll() {
        cache.clear();
    }

    private ApiClient build(K8sClusterRef ref) throws IOException {
        Path p = Paths.get(ref.kubeconfigPath());
        if (!Files.isRegularFile(p)) {
            throw new IOException("kubeconfig not found: " + ref.kubeconfigPath());
        }
        KubeConfig kc;
        try (FileReader r = new FileReader(p.toFile())) {
            kc = KubeConfig.loadKubeConfig(r);
        }
        // Kubeconfig context selection. The official loader defaults
        // to `current-context`, which may not match the ref the user
        // picked — always override explicitly.
        if (!kc.setContext(ref.contextName())) {
            throw new IOException("context '" + ref.contextName()
                    + "' not present in " + ref.kubeconfigPath());
        }
        // The loader resolves relative paths inside the kubeconfig
        // against cwd by default, which breaks when our cwd differs
        // from the kubeconfig's directory. Point it at the config's
        // own directory so exec/cert paths resolve correctly.
        kc.setFile(p.toFile());

        ApiClient client;
        try {
            client = ClientBuilder.kubeconfig(kc).build();
        } catch (IOException ioe) {
            throw ioe;
        }
        // Slightly longer than the default 10 s so cold exec-plugin
        // warm-up (aws-iam-authenticator's first STS call can push
        // 3-4 s) doesn't flap.
        client.setReadTimeout(DEFAULT_READ_TIMEOUT_MS);
        log.debug("built ApiClient for {} ({} users in config)",
                ref.coordinates(), kc.getUsers() == null ? 0 : kc.getUsers().size());
        return client;
    }

    private static long safeMtime(String path) {
        try {
            return Files.getLastModifiedTime(Paths.get(path)).toMillis();
        } catch (IOException e) {
            return -1L;
        }
    }

    private record CacheKey(String kubeconfigPath, String contextName) {}

    private record Cached(ApiClient client, long mtime) {}
}
