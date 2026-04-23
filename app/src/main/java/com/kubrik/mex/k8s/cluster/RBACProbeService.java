package com.kubrik.mex.k8s.cluster;

import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.model.RBACPermissions;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AuthorizationV1Api;
import io.kubernetes.client.openapi.models.V1ResourceAttributes;
import io.kubernetes.client.openapi.models.V1SelfSubjectAccessReview;
import io.kubernetes.client.openapi.models.V1SelfSubjectAccessReviewSpec;
import io.kubernetes.client.openapi.models.V1SubjectAccessReviewStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * v2.8.1 Q2.8.1-A3 — Reports the user's permissions in a given
 * namespace via {@code SelfSubjectAccessReview}.
 *
 * <p>SSAR is a server-side dry-run — there's no cluster-admin required
 * to issue it, and the API server itself evaluates the same rules it
 * would during a real request. This is the only acceptable way to
 * answer "can I do X?" without actually attempting X.</p>
 *
 * <p>We probe the facts the provisioning path needs (plus a couple
 * cluster-scoped niceties used by the wizard target step). Each
 * check is independent: a failed single review doesn't fail the
 * whole probe — the UI simply renders that row with an unknown
 * status.</p>
 *
 * <p>The CRDs we key on are the well-known ones: MongoDB Community
 * Operator (group {@code mongodbcommunity.mongodb.com}, resource
 * {@code mongodbcommunity}) and Percona Server for MongoDB (group
 * {@code psmdb.percona.com}, resource {@code perconaservermongodbs}).
 * "Can I create this resource?" transparently reports <em>false</em>
 * when the CRD itself is absent — Kubernetes treats the unknown
 * resource as not-permitted, which is exactly the pre-flight signal
 * we want.</p>
 */
public final class RBACProbeService {

    private static final Logger log = LoggerFactory.getLogger(RBACProbeService.class);

    public static final String MCO_GROUP = "mongodbcommunity.mongodb.com";
    public static final String MCO_RESOURCE = "mongodbcommunity";
    public static final String PSMDB_GROUP = "psmdb.percona.com";
    public static final String PSMDB_RESOURCE = "perconaservermongodbs";

    private final KubeClientFactory clientFactory;

    public RBACProbeService(KubeClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    public RBACPermissions probe(K8sClusterRef ref, String namespace) throws IOException {
        if (namespace == null || namespace.isBlank()) namespace = "default";
        ApiClient client = clientFactory.get(ref);
        AuthorizationV1Api api = new AuthorizationV1Api(client);

        boolean canListPods        = can(api, "list", "",             "pods",              namespace);
        boolean canReadEvents      = can(api, "list", "",             "events",            namespace);
        boolean canReadSecrets     = can(api, "get",  "",             "secrets",           namespace);
        boolean canCreateSecrets   = can(api, "create","",            "secrets",           namespace);
        boolean canCreatePvcs      = can(api, "create","",            "persistentvolumeclaims", namespace);
        boolean canCreateMcoCr     = can(api, "create", MCO_GROUP,    MCO_RESOURCE,        namespace);
        boolean canCreatePsmdbCr   = can(api, "create", PSMDB_GROUP,  PSMDB_RESOURCE,      namespace);
        boolean canListNamespaces  = can(api, "list", "",             "namespaces",        null);
        boolean canCreateNamespace = can(api, "create","",            "namespaces",        null);

        return new RBACPermissions(
                namespace,
                canListPods, canReadEvents,
                canReadSecrets, canCreateSecrets,
                canCreatePvcs,
                canCreateMcoCr, canCreatePsmdbCr,
                canListNamespaces, canCreateNamespace);
    }

    private static boolean can(AuthorizationV1Api api,
                                String verb, String group, String resource, String namespace) {
        V1ResourceAttributes attrs = new V1ResourceAttributes()
                .verb(verb)
                .group(group == null ? "" : group)
                .resource(resource);
        if (namespace != null) attrs.namespace(namespace);
        V1SelfSubjectAccessReview body = new V1SelfSubjectAccessReview()
                .spec(new V1SelfSubjectAccessReviewSpec().resourceAttributes(attrs));
        try {
            V1SelfSubjectAccessReview result = api.createSelfSubjectAccessReview(body).execute();
            V1SubjectAccessReviewStatus status = result == null ? null : result.getStatus();
            return status != null && Boolean.TRUE.equals(status.getAllowed());
        } catch (ApiException ae) {
            // 403 on SSAR itself is rare — the authorization API is
            // normally open to every authenticated user. Treat any
            // failure as "unknown → not allowed" so pre-flight errs
            // safe.
            log.debug("SSAR {} {}/{} in {} failed: HTTP {}",
                    verb, group, resource, namespace, ae.getCode());
            return false;
        } catch (Exception e) {
            log.debug("SSAR {} {}/{} in {} errored: {}",
                    verb, group, resource, namespace, e.toString());
            return false;
        }
    }
}
