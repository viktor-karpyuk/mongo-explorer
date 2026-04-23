package com.kubrik.mex.k8s.preflight.checks;

import com.kubrik.mex.k8s.cluster.RBACProbeService;
import com.kubrik.mex.k8s.model.RBACPermissions;
import com.kubrik.mex.k8s.preflight.PreflightCheck;
import com.kubrik.mex.k8s.preflight.PreflightResult;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import io.kubernetes.client.openapi.ApiClient;

import java.util.ArrayList;
import java.util.List;

/**
 * v2.8.1 Q2.8.1-G — Sufficient RBAC in the target namespace.
 *
 * <p>Runs the {@link RBACProbeService} SSAR batch and folds the
 * booleans into a single summary. Any missing create/read on pods,
 * events, secrets, PVCs, or the operator CR → FAIL with a list of
 * the specific misses.</p>
 */
public final class RbacCheck implements PreflightCheck {

    public static final String ID = "preflight.rbac";

    @Override public String id() { return ID; }
    @Override public String label() { return "RBAC sufficient"; }
    @Override public PreflightScope scope(ProvisionModel m) { return PreflightScope.ALWAYS; }

    @Override
    public PreflightResult run(ApiClient client, ProvisionModel m) {
        RBACPermissions perms;
        try {
            // Build a tiny synthetic ref pointing at the fresh client;
            // probe uses the client directly via the same factory the
            // wizard passed in. For pre-flight we shortcut by
            // re-using the service with an in-place factory wrapper.
            RBACProbeService probe = new RBACProbeService(new InPlaceFactory(client));
            perms = probe.probe(syntheticRef(m), m.namespace());
        } catch (Exception e) {
            return PreflightResult.fail(ID,
                    "RBAC probe failed: " + e.getMessage(),
                    "Verify the cluster is reachable and the ServiceAccount can issue SelfSubjectAccessReviews.");
        }

        List<String> missing = new ArrayList<>();
        if (!perms.canListPods()) missing.add("list pods");
        if (!perms.canReadEvents()) missing.add("list events");
        if (!perms.canReadSecrets()) missing.add("get secrets");
        if (!perms.canCreateSecrets()) missing.add("create secrets");
        if (!perms.canCreatePvcs()) missing.add("create persistentvolumeclaims");

        boolean canOperatorCr = switch (m.operator()) {
            case MCO -> perms.canCreateMcoCr();
            case PSMDB -> perms.canCreatePsmdbCr();
        };
        if (!canOperatorCr) {
            missing.add("create " + (m.operator() == com.kubrik.mex.k8s.provision.OperatorId.MCO
                    ? "mongodbcommunity.mongodbcommunity.mongodb.com"
                    : "perconaservermongodbs.psmdb.percona.com"));
        }

        if (missing.isEmpty()) return PreflightResult.pass(ID);
        return PreflightResult.fail(ID,
                "Caller can't: " + String.join(", ", missing) + ".",
                "Grant the ServiceAccount an appropriate Role / RoleBinding for namespace "
                        + m.namespace() + ".");
    }

    /* Test-friendly helpers kept private — RBACProbeService expects a KubeClientFactory; we
     * synthesise the minimal thing that lets the probe re-use the client the engine passes. */
    private static com.kubrik.mex.k8s.model.K8sClusterRef syntheticRef(ProvisionModel m) {
        return new com.kubrik.mex.k8s.model.K8sClusterRef(
                m.clusterId(), "preflight", "n/a", "n/a",
                java.util.Optional.empty(), java.util.Optional.empty(),
                0L, java.util.Optional.empty());
    }

    private static final class InPlaceFactory extends com.kubrik.mex.k8s.client.KubeClientFactory {
        private final ApiClient client;
        InPlaceFactory(ApiClient c) { this.client = c; }
        @Override public ApiClient get(com.kubrik.mex.k8s.model.K8sClusterRef ref) { return client; }
        @Override public ApiClient fresh(com.kubrik.mex.k8s.model.K8sClusterRef ref) { return client; }
    }
}
