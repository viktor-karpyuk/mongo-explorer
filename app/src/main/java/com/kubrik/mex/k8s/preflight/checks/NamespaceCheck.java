package com.kubrik.mex.k8s.preflight.checks;

import com.kubrik.mex.k8s.preflight.PreflightCheck;
import com.kubrik.mex.k8s.preflight.PreflightResult;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;

/**
 * v2.8.1 Q2.8.1-G — Target namespace exists, or the wizard's
 * "create namespace" flag is on.
 *
 * <p>Missing ns + {@code createNamespace = false} → FAIL. Missing ns
 * + flag on → PASS (Apply will create it). Present → PASS.</p>
 */
public final class NamespaceCheck implements PreflightCheck {

    public static final String ID = "preflight.namespace";

    @Override public String id() { return ID; }
    @Override public String label() { return "Target namespace"; }
    @Override public PreflightScope scope(ProvisionModel m) { return PreflightScope.ALWAYS; }

    @Override
    public PreflightResult run(ApiClient client, ProvisionModel m) {
        try {
            new CoreV1Api(client).readNamespace(m.namespace()).execute();
            return PreflightResult.pass(ID);
        } catch (ApiException ae) {
            if (ae.getCode() == 404) {
                if (m.createNamespace()) return PreflightResult.pass(ID);
                return PreflightResult.fail(ID,
                        "Namespace '" + m.namespace() + "' does not exist.",
                        "Toggle \"Create namespace\" in the Target step, "
                        + "or pick an existing one.");
            }
            return PreflightResult.fail(ID,
                    "Namespace probe failed: HTTP " + ae.getCode(),
                    "Verify the ServiceAccount can read namespaces.");
        } catch (Exception e) {
            return PreflightResult.fail(ID,
                    "Namespace probe errored: " + e.getMessage(),
                    "Cluster unreachable — retry.");
        }
    }
}
