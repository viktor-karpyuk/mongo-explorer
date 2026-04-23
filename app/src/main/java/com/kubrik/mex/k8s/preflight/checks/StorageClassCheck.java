package com.kubrik.mex.k8s.preflight.checks;

import com.kubrik.mex.k8s.preflight.PreflightCheck;
import com.kubrik.mex.k8s.preflight.PreflightResult;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.StorageV1Api;
import io.kubernetes.client.openapi.models.V1StorageClass;
import io.kubernetes.client.openapi.models.V1StorageClassList;

import java.util.Objects;

/**
 * v2.8.1 Q2.8.1-G — Verify the requested StorageClass exists, or
 * the cluster has an annotated default when the model leaves it
 * empty.
 *
 * <p>Absence of a default + no explicit class → FAIL. Specific
 * class not present → FAIL. Explicit class present → PASS.</p>
 */
public final class StorageClassCheck implements PreflightCheck {

    public static final String ID = "preflight.storage-class";
    private static final String DEFAULT_ANNOTATION =
            "storageclass.kubernetes.io/is-default-class";

    @Override public String id() { return ID; }
    @Override public String label() { return "StorageClass present"; }
    @Override public PreflightScope scope(ProvisionModel m) { return PreflightScope.ALWAYS; }

    @Override
    public PreflightResult run(ApiClient client, ProvisionModel m) {
        V1StorageClassList list;
        try {
            list = new StorageV1Api(client).listStorageClass().execute();
        } catch (ApiException ae) {
            return PreflightResult.fail(ID,
                    "StorageClass list failed: HTTP " + ae.getCode(),
                    "Grant the caller permission to list storageclasses.");
        } catch (Exception e) {
            return PreflightResult.fail(ID,
                    "StorageClass list errored: " + e.getMessage(),
                    "Cluster unreachable — retry.");
        }

        String wanted = m.storage().storageClass().orElse(null);
        if (wanted != null) {
            for (V1StorageClass sc : list.getItems()) {
                if (sc.getMetadata() != null && wanted.equals(sc.getMetadata().getName())) {
                    return PreflightResult.pass(ID);
                }
            }
            return PreflightResult.fail(ID,
                    "StorageClass '" + wanted + "' not found.",
                    "Existing classes: " + existingNames(list));
        }
        // No explicit class — need a default.
        for (V1StorageClass sc : list.getItems()) {
            if (sc.getMetadata() != null
                    && "true".equals(Objects.toString(
                            sc.getMetadata().getAnnotations() == null ? null
                                    : sc.getMetadata().getAnnotations().get(DEFAULT_ANNOTATION),
                            null))) {
                return PreflightResult.pass(ID);
            }
        }
        return PreflightResult.fail(ID,
                "No default StorageClass on the cluster.",
                "Either pick an explicit class or annotate one with "
                + DEFAULT_ANNOTATION + "=true.");
    }

    private static String existingNames(V1StorageClassList list) {
        StringBuilder sb = new StringBuilder();
        for (V1StorageClass sc : list.getItems()) {
            if (sc.getMetadata() == null || sc.getMetadata().getName() == null) continue;
            if (sb.length() > 0) sb.append(", ");
            sb.append(sc.getMetadata().getName());
        }
        return sb.length() == 0 ? "(none)" : sb.toString();
    }
}
