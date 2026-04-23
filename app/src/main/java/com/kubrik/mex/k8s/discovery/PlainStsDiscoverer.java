package com.kubrik.mex.k8s.discovery;

import com.kubrik.mex.k8s.model.DiscoveredMongo;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1StatefulSetList;
import io.kubernetes.client.openapi.models.V1StatefulSetSpec;
import io.kubernetes.client.openapi.models.V1StatefulSetStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * v2.8.1 Q2.8.1-B1 — Discovers "plain" Mongo workloads: StatefulSets
 * whose pod template references a Mongo-or-Percona image.
 *
 * <p>This is deliberately heuristic. Operator-managed CRs emit
 * StatefulSets too, so we exclude rows that look operator-owned by
 * checking label and ownerReference patterns we know about. What
 * remains is "someone deployed mongod from a chart or hand-rolled
 * StatefulSet," and that's what we want to surface for "connect to
 * existing."</p>
 *
 * <p>Topology is guessed from the StatefulSet's replica count:
 * 1 → standalone; 3 / 5 → replset; anything else → UNKNOWN. The
 * auth kind is always {@code UNKNOWN} — plain STS gives us no CR
 * surface to peek at; the secret-pickup step in B2 lets the user
 * map credentials by hand.</p>
 */
public final class PlainStsDiscoverer {

    private static final Logger log = LoggerFactory.getLogger(PlainStsDiscoverer.class);

    /** Image-tag substrings we consider Mongo. */
    static final Set<String> MONGO_IMAGE_MARKERS = Set.of(
            "mongo", "percona/percona-server-mongodb"
    );

    /** Labels that typically mean "this STS is owned by an operator CR." */
    private static final Set<String> OPERATOR_OWNED_LABELS = Set.of(
            "app.kubernetes.io/managed-by", // mongodb community operator
            "app.kubernetes.io/instance",   // PSMDB + many charts
            "mongodb.percona.com/cluster-name"
    );

    /**
     * List namespaces visible to the caller, then list STS within
     * each. Per-namespace listing is used so an RBAC-restricted
     * ServiceAccount can still return partial results — we skip
     * namespaces that 403 us.
     */
    public List<DiscoveredMongo> discover(ApiClient client, long clusterId) throws ApiException {
        Objects.requireNonNull(client, "client");
        CoreV1Api core = new CoreV1Api(client);
        AppsV1Api apps = new AppsV1Api(client);
        V1NamespaceList namespaces;
        try {
            namespaces = core.listNamespace().execute();
        } catch (ApiException ae) {
            if (ae.getCode() == 403) {
                log.debug("caller can't list namespaces on cluster {}", clusterId);
                return List.of();
            }
            throw ae;
        }

        List<DiscoveredMongo> out = new ArrayList<>();
        for (V1Namespace ns : namespaces.getItems()) {
            if (ns.getMetadata() == null || ns.getMetadata().getName() == null) continue;
            String name = ns.getMetadata().getName();
            V1StatefulSetList stsList;
            try {
                stsList = apps.listNamespacedStatefulSet(name).execute();
            } catch (ApiException ae) {
                // 403 here means "caller can list namespaces but not STS in this
                // one" — ignore and continue, partial is better than nothing.
                if (ae.getCode() == 403) continue;
                throw ae;
            }
            for (V1StatefulSet sts : stsList.getItems()) {
                DiscoveredMongo d = tryParse(sts, clusterId);
                if (d != null) out.add(d);
            }
        }
        return Collections.unmodifiableList(out);
    }

    static DiscoveredMongo tryParse(V1StatefulSet sts, long clusterId) {
        if (sts.getMetadata() == null) return null;
        String name = sts.getMetadata().getName();
        String namespace = sts.getMetadata().getNamespace();
        if (name == null || namespace == null) return null;

        if (isOperatorOwned(sts)) return null;

        V1StatefulSetSpec spec = sts.getSpec();
        if (spec == null) return null;
        V1PodTemplateSpec tpl = spec.getTemplate();
        if (tpl == null || tpl.getSpec() == null) return null;

        String mongoImage = firstMongoImage(tpl.getSpec().getContainers());
        if (mongoImage == null) return null;

        int replicas = spec.getReplicas() == null ? 0 : spec.getReplicas();
        DiscoveredMongo.Topology topology = switch (replicas) {
            case 1 -> DiscoveredMongo.Topology.STANDALONE;
            case 3 -> DiscoveredMongo.Topology.RS3;
            case 5 -> DiscoveredMongo.Topology.RS5;
            default -> DiscoveredMongo.Topology.UNKNOWN;
        };

        // Plain STS may or may not have a matching Service; if users
        // expose it conventionally as `<name>` that's a safe default,
        // otherwise the B3 connect flow lets them pick a Service.
        Optional<String> svcName = Optional.of(
                spec.getServiceName() == null ? name : spec.getServiceName());

        V1StatefulSetStatus status = sts.getStatus();
        Optional<Boolean> ready = Optional.empty();
        if (status != null && status.getReplicas() != null && status.getReadyReplicas() != null) {
            ready = Optional.of(status.getReadyReplicas().equals(status.getReplicas()));
        }

        String version = imageTag(mongoImage);

        return new DiscoveredMongo(
                clusterId,
                DiscoveredMongo.Origin.PLAIN_STS,
                namespace, name,
                topology,
                svcName,
                Optional.of(27017),
                DiscoveredMongo.AuthKind.UNKNOWN,
                ready,
                Optional.ofNullable(version),
                Optional.empty(),
                Optional.of(name));
    }

    static boolean isOperatorOwned(V1StatefulSet sts) {
        if (sts.getMetadata() == null) return false;
        // ownerReferences with controller=true → operator-created.
        if (sts.getMetadata().getOwnerReferences() != null) {
            for (var or : sts.getMetadata().getOwnerReferences()) {
                if (Boolean.TRUE.equals(or.getController())) return true;
            }
        }
        // Label heuristics — recognise operators by a known label's
        // presence. This is soft: we only bail when the label value
        // names a Mongo operator we ship support for.
        if (sts.getMetadata().getLabels() != null) {
            String managedBy = sts.getMetadata().getLabels()
                    .get("app.kubernetes.io/managed-by");
            if (managedBy != null) {
                String lower = managedBy.toLowerCase();
                if (lower.contains("mongodb") || lower.contains("percona")) return true;
            }
        }
        return false;
    }

    static String firstMongoImage(List<V1Container> containers) {
        if (containers == null) return null;
        for (V1Container c : containers) {
            if (c.getImage() == null) continue;
            String image = c.getImage().toLowerCase();
            for (String marker : MONGO_IMAGE_MARKERS) {
                if (image.contains(marker)) return c.getImage();
            }
        }
        return null;
    }

    static String imageTag(String image) {
        if (image == null) return null;
        int idx = image.lastIndexOf(':');
        if (idx < 0 || idx == image.length() - 1) return null;
        return image.substring(idx + 1);
    }
}
