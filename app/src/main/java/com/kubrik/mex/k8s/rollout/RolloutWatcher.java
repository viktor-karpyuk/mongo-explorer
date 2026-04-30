package com.kubrik.mex.k8s.rollout;

import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.operator.DeploymentStatus;
import com.kubrik.mex.k8s.operator.OperatorAdapter;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * v2.8.1 Q2.8.1-L — Polls a provisioning row's CR status until the
 * operator lands it on READY or FAILED.
 *
 * <p>Three inputs:</p>
 * <ul>
 *   <li>{@link K8sClusterRef} — the cluster to poll against.</li>
 *   <li>The deployment's namespace + name.</li>
 *   <li>{@link OperatorAdapter} — supplies {@code crdGroup()} /
 *       {@code crdKind()} so we know which CR to read, and
 *       {@code parseStatus()} to convert the raw CR + pods list
 *       into {@link DeploymentStatus}.</li>
 * </ul>
 *
 * <p>Polls every {@link #pollIntervalMs}; times out at {@link
 * #timeoutMs}. Each poll converts the adapter's answer into a
 * {@link RolloutEvent} (INFO / WARN / ERROR) so the rollout viewer
 * shows progression, and returns the final decision to the caller.</p>
 *
 * <p>Pure watcher — doesn't touch the provisioning_records row. The
 * caller (typically {@link com.kubrik.mex.k8s.apply.ApplyOrchestrator})
 * translates the decision into {@code markReady} / {@code markFailed}
 * so the SQLite lifecycle stays in one place.</p>
 */
public final class RolloutWatcher {

    private static final Logger log = LoggerFactory.getLogger(RolloutWatcher.class);

    /** Gson is documented thread-safe + its construction walks every
     *  registered TypeAdapterFactory, so a shared instance saves a
     *  non-trivial allocation per provision. */
    private static final Gson SHARED_GSON = new Gson();

    /** Default: 3s between polls — matches MCO / PSMDB status-refresh cadence. */
    public static final long DEFAULT_POLL_INTERVAL_MS = 3_000L;
    /** Default: 15 min for a full provision to stabilise on kind / dev clusters. */
    public static final long DEFAULT_TIMEOUT_MS = 15 * 60 * 1_000L;

    private final KubeClientFactory clientFactory;
    private final long pollIntervalMs;
    private final long timeoutMs;

    public RolloutWatcher(KubeClientFactory clientFactory) {
        this(clientFactory, DEFAULT_POLL_INTERVAL_MS, DEFAULT_TIMEOUT_MS);
    }

    public RolloutWatcher(KubeClientFactory clientFactory, long pollIntervalMs, long timeoutMs) {
        this.clientFactory = Objects.requireNonNull(clientFactory, "clientFactory");
        if (pollIntervalMs <= 0) throw new IllegalArgumentException("pollIntervalMs > 0");
        if (timeoutMs <= 0) throw new IllegalArgumentException("timeoutMs > 0");
        this.pollIntervalMs = pollIntervalMs;
        this.timeoutMs = timeoutMs;
    }

    /**
     * Block until the CR reaches READY or FAILED, or the timeout
     * expires. {@link EventSink} receives one
     * {@link RolloutEvent} per poll where the status changed,
     * plus a final terminal event when we return.
     *
     * @return the last known status. On timeout this is whatever
     *         the last poll returned — callers typically treat
     *         {@code APPLYING} after timeout as {@code FAILED}.
     */
    public WatchResult watch(K8sClusterRef ref,
                              OperatorAdapter adapter,
                              String namespace,
                              String name,
                              long provisioningId,
                              EventSink sink) {
        return watch(ref, adapter, namespace, name, provisioningId, sink, null);
    }

    /**
     * Same as {@link #watch(K8sClusterRef, OperatorAdapter, String,
     * String, long, EventSink)} but invokes {@code extension} once per
     * tick (after the CR-status poll) so callers can fold in side
     * channels like {@link
     * com.kubrik.mex.k8s.compute.karpenter.KarpenterEventProbe}.
     * Extension exceptions are logged + swallowed so an unhealthy side
     * channel can't stall the main rollout watcher.
     */
    public WatchResult watch(K8sClusterRef ref,
                              OperatorAdapter adapter,
                              String namespace,
                              String name,
                              long provisioningId,
                              EventSink sink,
                              PollExtension extension) {
        ApiClient client;
        try {
            client = clientFactory.get(ref);
        } catch (Exception e) {
            sink.emit(error(provisioningId, "KubeconfigError", e.getMessage()));
            return new WatchResult(DeploymentStatus.FAILED, 0, "kubeconfig: " + e.getMessage());
        }
        String plural = pluralFor(adapter.crdKind());
        DynamicKubernetesApi crApi = new DynamicKubernetesApi(
                adapter.crdGroup(), "v1", plural, client);

        long deadline = System.currentTimeMillis() + timeoutMs;
        DeploymentStatus lastStatus = DeploymentStatus.UNKNOWN;
        int polls = 0;
        Gson gson = SHARED_GSON;

        while (System.currentTimeMillis() < deadline) {
            polls++;
            DeploymentStatus status;
            try {
                Map<String, Object> crStatus = readCrStatus(crApi, namespace, name, gson);
                List<Map<String, Object>> pods = listPods(client, namespace, name, gson);
                status = adapter.parseStatus(crStatus, pods, List.of());
            } catch (Exception e) {
                log.debug("watch poll {}/{}: {}", namespace, name, e.toString());
                sink.emit(warn(provisioningId, "PollError", e.getMessage()));
                status = DeploymentStatus.UNKNOWN;
            }

            if (status != lastStatus) {
                sink.emit(info(provisioningId,
                        "StatusChange", lastStatus + " → " + status));
                lastStatus = status;
            }
            if (extension != null) {
                try { extension.poll(sink); }
                catch (Exception ext) {
                    log.debug("watch extension {}/{}: {}",
                            namespace, name, ext.toString());
                }
            }
            if (status == DeploymentStatus.READY) {
                sink.emit(info(provisioningId, "Ready", "deployment is healthy"));
                return new WatchResult(DeploymentStatus.READY, polls, "ready after " + polls + " poll(s)");
            }
            if (status == DeploymentStatus.FAILED) {
                sink.emit(error(provisioningId, "Failed", "operator reported FAILED"));
                return new WatchResult(DeploymentStatus.FAILED, polls, "failed after " + polls + " poll(s)");
            }
            try { Thread.sleep(pollIntervalMs); }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return new WatchResult(lastStatus, polls, "interrupted");
            }
        }
        sink.emit(warn(provisioningId, "Timeout",
                "still " + lastStatus + " after " + (timeoutMs / 1000) + "s"));
        return new WatchResult(lastStatus, polls, "timed out");
    }

    /* ============================ helpers ============================ */

    private Map<String, Object> readCrStatus(DynamicKubernetesApi api,
                                               String namespace, String name, Gson gson) {
        KubernetesApiResponse<DynamicKubernetesObject> res = api.get(namespace, name);
        if (!res.isSuccess() || res.getObject() == null) return Map.of();
        JsonObject root = res.getObject().getRaw();
        if (root == null || !root.has("status")) return Map.of();
        return gsonToMap(root.getAsJsonObject("status"), gson);
    }

    private List<Map<String, Object>> listPods(ApiClient client, String namespace,
                                                  String crName, Gson gson) throws ApiException {
        // The operators both label their pods with the CR name via
        // app.kubernetes.io/instance — use that selector so we only
        // capture the pods related to this provision.
        String selector = "app.kubernetes.io/instance=" + crName;
        V1PodList pods = new CoreV1Api(client)
                .listNamespacedPod(namespace)
                .labelSelector(selector)
                .execute();
        List<Map<String, Object>> out = new ArrayList<>(pods.getItems().size());
        for (V1Pod pod : pods.getItems()) {
            String json = client.getJSON().serialize(pod);
            out.add(gsonToMap(com.google.gson.JsonParser.parseString(json)
                    .getAsJsonObject(), gson));
        }
        return out;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Map<String, Object> gsonToMap(JsonObject obj, Gson gson) {
        if (obj == null) return Map.of();
        return (Map) gson.fromJson(obj, Map.class);
    }

    /** Same table the apply opener uses; keep in sync. */
    static String pluralFor(String kind) {
        return switch (kind) {
            case "MongoDBCommunity" -> "mongodbcommunity";
            case "PerconaServerMongoDB" -> "perconaservermongodbs";
            default -> kind.toLowerCase(java.util.Locale.ROOT) + "s";
        };
    }

    private static RolloutEvent info(long id, String reason, String msg) {
        return new RolloutEvent(id, System.currentTimeMillis(),
                RolloutEvent.Source.CR_STATUS, RolloutEvent.Severity.INFO,
                java.util.Optional.of(reason), java.util.Optional.of(msg),
                java.util.Optional.empty());
    }

    private static RolloutEvent warn(long id, String reason, String msg) {
        return new RolloutEvent(id, System.currentTimeMillis(),
                RolloutEvent.Source.CR_STATUS, RolloutEvent.Severity.WARN,
                java.util.Optional.of(reason), java.util.Optional.of(msg),
                java.util.Optional.empty());
    }

    private static RolloutEvent error(long id, String reason, String msg) {
        return new RolloutEvent(id, System.currentTimeMillis(),
                RolloutEvent.Source.CR_STATUS, RolloutEvent.Severity.ERROR,
                java.util.Optional.of(reason), java.util.Optional.of(msg),
                java.util.Optional.empty());
    }

    /** Pluggable sink so callers can persist + publish per event. */
    public interface EventSink {
        void emit(RolloutEvent event);
    }

    /** Per-tick side-channel hook. Invoked once per poll after the
     *  CR-status read so probes can emit their own
     *  {@link RolloutEvent}s on the same cadence as the main watcher. */
    @FunctionalInterface
    public interface PollExtension {
        void poll(EventSink sink) throws Exception;
    }

    /** Final outcome returned by {@link #watch}. */
    public record WatchResult(DeploymentStatus status, int pollsUsed, String summary) {
        public boolean ok() { return status == DeploymentStatus.READY; }
    }
}
