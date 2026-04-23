package com.kubrik.mex.k8s.preflight.checks;

import com.kubrik.mex.k8s.preflight.PreflightCheck;
import com.kubrik.mex.k8s.preflight.PreflightResult;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.k8s.provision.TlsSpec;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.ApiextensionsV1Api;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;

/**
 * v2.8.1 Q2.8.1-G — cert-manager Issuer exists in the target ns.
 *
 * <p>Runs only when {@code tls.mode = CERT_MANAGER}. Missing CRD →
 * FAIL (cert-manager not installed). CRD present but no Issuer with
 * the user-named name in the ns → FAIL. Issuer present → PASS.</p>
 */
public final class CertManagerCheck implements PreflightCheck {

    public static final String ID = "preflight.cert-manager";

    @Override public String id() { return ID; }
    @Override public String label() { return "cert-manager Issuer"; }

    @Override
    public PreflightScope scope(ProvisionModel m) {
        return m.tls().mode() == TlsSpec.Mode.CERT_MANAGER
                ? PreflightScope.CONDITIONAL
                : PreflightScope.SKIP;
    }

    @Override
    public PreflightResult run(ApiClient client, ProvisionModel m) {
        String issuerName = m.tls().certManagerIssuer().orElse("");
        if (issuerName.isBlank()) {
            return PreflightResult.fail(ID,
                    "TLS mode is cert-manager but no Issuer name is set.",
                    "Pick an Issuer in the TLS step.");
        }
        // Probe CRD first — gives a distinct "cert-manager not installed" error.
        try {
            new ApiextensionsV1Api(client)
                    .readCustomResourceDefinition("issuers.cert-manager.io").execute();
        } catch (ApiException ae) {
            if (ae.getCode() == 404) {
                return PreflightResult.fail(ID,
                        "cert-manager is not installed on the cluster.",
                        "Install cert-manager: https://cert-manager.io/docs/installation/");
            }
            return PreflightResult.warn(ID,
                    "cert-manager CRD probe failed: HTTP " + ae.getCode(),
                    "Pre-flight can't confirm; Apply may still succeed.");
        } catch (Exception e) {
            return PreflightResult.warn(ID,
                    "cert-manager CRD probe errored: " + e.getMessage(),
                    null);
        }

        try {
            CustomObjectsApi co = new CustomObjectsApi(client);
            co.getNamespacedCustomObject("cert-manager.io", "v1", m.namespace(),
                    "issuers", issuerName).execute();
            return PreflightResult.pass(ID);
        } catch (ApiException ae) {
            if (ae.getCode() == 404) {
                return PreflightResult.fail(ID,
                        "Issuer '" + issuerName + "' not found in namespace "
                        + m.namespace() + ".",
                        "Create the Issuer first or pick a different name.");
            }
            return PreflightResult.warn(ID,
                    "Issuer probe failed: HTTP " + ae.getCode(),
                    "Pre-flight can't confirm; verify manually.");
        } catch (Exception e) {
            return PreflightResult.warn(ID,
                    "Issuer probe errored: " + e.getMessage(), null);
        }
    }
}
