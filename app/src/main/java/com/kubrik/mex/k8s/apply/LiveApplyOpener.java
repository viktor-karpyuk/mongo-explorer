package com.kubrik.mex.k8s.apply;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.kubrik.mex.k8s.rollout.ResourceCatalogue;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * v2.8.1 Q2.8.1-L — Production implementation of
 * {@link ApplyOrchestrator.ApplyOpener}.
 *
 * <p>Uses {@link DynamicKubernetesApi} so we can apply arbitrary
 * kinds (including our operator CRDs) without pre-generating typed
 * stubs. Two responsibilities:</p>
 * <ul>
 *   <li><b>Apply</b> — create-or-update a single Kubernetes object
 *       from a YAML document. Converts the YAML to a
 *       {@link DynamicKubernetesObject}, issues {@code create}, and
 *       on 409 Conflict falls back to {@code update} so subsequent
 *       re-applies after a failed rollout are idempotent.</li>
 *   <li><b>Delete</b> — remove a named object by its
 *       {@link ResourceCatalogue.Ref}. Absence (404) is treated as
 *       success so cleanup can iterate through a partial catalogue
 *       without tripping on already-gone resources.</li>
 * </ul>
 *
 * <p>The opener caches one {@link DynamicKubernetesApi} per
 * {@code (group, version, plural)} triple so it doesn't rebuild the
 * generic wrapper for every document — the wrapper holds an
 * {@code okhttp} call builder we want to reuse.</p>
 *
 * <p>The Kubernetes resource-plural of a kind is inferred from a
 * fixed table keyed on {@code apiVersion + kind} that covers every
 * kind the v2.8.1 renderers emit. Unknown kinds fall back to a
 * "lowercase kind + s" heuristic, which works for every core kind
 * we might pick up in an adversarial test; failures surface as
 * a clear {@link ApiException} at apply time.</p>
 */
public final class LiveApplyOpener implements ApplyOrchestrator.ApplyOpener {

    private static final Logger log = LoggerFactory.getLogger(LiveApplyOpener.class);

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    /**
     * Kind → resource plural for the kinds our renderers emit. The
     * Kubernetes API server discovers these at runtime via
     * {@code /apis/<group>/<version>}, but the Java dynamic client
     * expects the plural pre-computed.
     */
    private static final Map<String, String> KIND_TO_PLURAL = Map.ofEntries(
            Map.entry("Secret", "secrets"),
            Map.entry("ConfigMap", "configmaps"),
            Map.entry("Namespace", "namespaces"),
            Map.entry("PersistentVolumeClaim", "persistentvolumeclaims"),
            Map.entry("Service", "services"),
            Map.entry("PodDisruptionBudget", "poddisruptionbudgets"),
            Map.entry("ServiceMonitor", "servicemonitors"),
            Map.entry("CronJob", "cronjobs"),
            Map.entry("Deployment", "deployments"),
            Map.entry("StatefulSet", "statefulsets"),
            Map.entry("MongoDBCommunity", "mongodbcommunity"),
            Map.entry("PerconaServerMongoDB", "perconaservermongodbs"),
            // v2.8.3 Q2.8.3-D — Karpenter NodePool joins the catalogue
            // so cleanup deletes it alongside the Mongo CR.
            Map.entry("NodePool", "nodepools"));

    private final ApiClient boundClient;
    private final ConcurrentMap<String, DynamicKubernetesApi> apis = new ConcurrentHashMap<>();

    /**
     * The caller supplies the {@link ApiClient} once — the orchestrator
     * hands the same client per apply step because the catalogue row is
     * tied to a single cluster ref. A new opener is cheap; rebuild per
     * provision flow rather than sharing across clusters.
     */
    public LiveApplyOpener(ApiClient client) {
        this.boundClient = Objects.requireNonNull(client, "client");
    }

    @Override
    public void apply(ApiClient client, ResourceCatalogue.Ref ref, String yaml) throws Exception {
        ApiClient resolved = client == null ? boundClient : client;
        DynamicKubernetesObject obj = toDynamic(yaml);
        String group = splitGroup(ref.apiVersion());
        String version = splitVersion(ref.apiVersion());
        String plural = pluralFor(ref.kind());

        DynamicKubernetesApi api = apiFor(resolved, group, version, plural);

        // Namespaced resources supply the namespace; cluster-scoped
        // (Namespace itself) ignore it.
        boolean namespaced = !"Namespace".equals(ref.kind());

        // GenericKubernetesApi.create takes only the object — the
        // namespace rides inside metadata.namespace.
        if (obj.getMetadata() != null && obj.getMetadata().getNamespace() == null && namespaced) {
            obj.getMetadata().setNamespace(ref.namespace());
        }
        KubernetesApiResponse<DynamicKubernetesObject> res = api.create(obj);

        if (res.isSuccess()) return;

        // Resource already exists → update. The server-side-apply path
        // would be cleaner but requires the caller to own a fieldManager
        // name; v2.8.1 Alpha uses classic create-or-update which is
        // sufficient for the provisioning flow (re-applies after a
        // failed rollout).
        if (res.getHttpStatusCode() == 409) {
            KubernetesApiResponse<DynamicKubernetesObject> existing = namespaced
                    ? api.get(ref.namespace(), ref.name())
                    : api.get(ref.name());
            if (existing.isSuccess()) {
                obj.getMetadata().setResourceVersion(existing.getObject().getMetadata().getResourceVersion());
                KubernetesApiResponse<DynamicKubernetesObject> updated = api.update(obj);
                if (updated.isSuccess()) return;
                throw new ApiException(updated.getHttpStatusCode(),
                        "update " + ref.kind() + "/" + ref.name() + " failed: "
                        + describe(updated));
            }
        }
        throw new ApiException(res.getHttpStatusCode(),
                "create " + ref.kind() + "/" + ref.name() + " failed: " + describe(res));
    }

    @Override
    public void delete(ApiClient client, ResourceCatalogue.Ref ref) throws Exception {
        ApiClient resolved = client == null ? boundClient : client;
        String group = splitGroup(ref.apiVersion());
        String version = splitVersion(ref.apiVersion());
        String plural = pluralFor(ref.kind());
        DynamicKubernetesApi api = apiFor(resolved, group, version, plural);
        boolean namespaced = !"Namespace".equals(ref.kind());
        KubernetesApiResponse<DynamicKubernetesObject> res = namespaced
                ? api.delete(ref.namespace(), ref.name())
                : api.delete(ref.name());
        if (res.isSuccess() || res.getHttpStatusCode() == 404) return;
        throw new ApiException(res.getHttpStatusCode(),
                "delete " + ref.kind() + "/" + ref.name() + " failed: " + describe(res));
    }

    /* ============================ helpers ============================ */

    private DynamicKubernetesApi apiFor(ApiClient client, String group, String version, String plural) {
        String key = group + "|" + version + "|" + plural;
        return apis.computeIfAbsent(key,
                k -> new DynamicKubernetesApi(group, version, plural, client));
    }

    /** YAML → Gson JsonObject → DynamicKubernetesObject. */
    static DynamicKubernetesObject toDynamic(String yaml) throws IOException {
        // Jackson → Map → Gson converts in one hop. DynamicKubernetesObject
        // expects a Gson JsonObject, not Jackson's JsonNode.
        Object parsed = YAML.readValue(yaml, Object.class);
        String json;
        try {
            json = new com.fasterxml.jackson.databind.ObjectMapper()
                    .writeValueAsString(parsed);
        } catch (com.fasterxml.jackson.core.JsonProcessingException jpe) {
            throw new IOException("re-serialise YAML→JSON: " + jpe.getMessage(), jpe);
        }
        JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
        return new DynamicKubernetesObject(obj);
    }

    static String splitGroup(String apiVersion) {
        if (apiVersion == null) return "";
        int slash = apiVersion.indexOf('/');
        return slash < 0 ? "" : apiVersion.substring(0, slash);
    }

    static String splitVersion(String apiVersion) {
        if (apiVersion == null) return "";
        int slash = apiVersion.indexOf('/');
        return slash < 0 ? apiVersion : apiVersion.substring(slash + 1);
    }

    static String pluralFor(String kind) {
        String table = KIND_TO_PLURAL.get(kind);
        if (table != null) return table;
        // Fallback: lowercase + 's'. Works for most kinds; rarely
        // needed since the renderers emit a fixed set.
        return kind.toLowerCase(Locale.ROOT) + "s";
    }

    private static String describe(KubernetesApiResponse<?> res) {
        if (res.getStatus() != null && res.getStatus().getMessage() != null) {
            return "HTTP " + res.getHttpStatusCode() + " " + res.getStatus().getMessage();
        }
        return "HTTP " + res.getHttpStatusCode();
    }

    /** Exposed for tests. */
    Map<String, DynamicKubernetesApi> apiCacheSnapshot() {
        return new HashMap<>(apis);
    }
}
