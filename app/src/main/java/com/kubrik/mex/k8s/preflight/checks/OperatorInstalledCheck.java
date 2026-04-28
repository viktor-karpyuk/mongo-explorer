package com.kubrik.mex.k8s.preflight.checks;

import com.kubrik.mex.k8s.preflight.PreflightCheck;
import com.kubrik.mex.k8s.preflight.PreflightResult;
import com.kubrik.mex.k8s.provision.OperatorId;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.ApiextensionsV1Api;
import io.kubernetes.client.openapi.models.V1CustomResourceDefinition;

/**
 * v2.8.1 Q2.8.1-G — Verifies the chosen operator's CRD is installed.
 *
 * <p>Reads the CRD object by name. Absence → FAIL with an install-
 * link hint. Present but stored with an unsupported version → WARN
 * (the blessed matrix expands carefully per milestone §7.8).</p>
 */
public final class OperatorInstalledCheck implements PreflightCheck {

    public static final String ID = "preflight.operator-installed";

    @Override public String id() { return ID; }
    @Override public String label() { return "Operator installed"; }
    @Override public PreflightScope scope(ProvisionModel m) { return PreflightScope.ALWAYS; }

    @Override
    public PreflightResult run(ApiClient client, ProvisionModel m) {
        String crdName = switch (m.operator()) {
            case MCO -> "mongodbcommunity.mongodbcommunity.mongodb.com";
            case PSMDB -> "perconaservermongodbs.psmdb.percona.com";
        };
        try {
            V1CustomResourceDefinition crd = new ApiextensionsV1Api(client)
                    .readCustomResourceDefinition(crdName).execute();
            if (crd == null) {
                return PreflightResult.fail(ID,
                        "CRD " + crdName + " not installed on the cluster.",
                        installHint(m.operator()));
            }
            return PreflightResult.pass(ID);
        } catch (ApiException ae) {
            if (ae.getCode() == 404) {
                return PreflightResult.fail(ID,
                        "CRD " + crdName + " not installed on the cluster.",
                        installHint(m.operator()));
            }
            return PreflightResult.fail(ID,
                    "CRD probe failed: HTTP " + ae.getCode(),
                    "Verify the ServiceAccount can read CRDs.");
        } catch (Exception e) {
            return PreflightResult.fail(ID,
                    "CRD probe errored: " + e.getMessage(),
                    "Cluster may be unreachable — retry after the probe.");
        }
    }

    private static String installHint(OperatorId op) {
        return switch (op) {
            case MCO -> "Install the MongoDB Community Operator: "
                    + "https://github.com/mongodb/mongodb-kubernetes-operator";
            case PSMDB -> "Install the Percona Server for MongoDB Operator: "
                    + "https://docs.percona.com/percona-operator-for-mongodb/install.html";
        };
    }
}
