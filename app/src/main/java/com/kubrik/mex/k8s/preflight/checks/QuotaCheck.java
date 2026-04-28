package com.kubrik.mex.k8s.preflight.checks;

import com.kubrik.mex.k8s.preflight.PreflightCheck;
import com.kubrik.mex.k8s.preflight.PreflightResult;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.k8s.provision.Topology;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ResourceQuota;
import io.kubernetes.client.openapi.models.V1ResourceQuotaList;

/**
 * v2.8.1 Q2.8.1-G — ResourceQuota headroom check.
 *
 * <p>Calculates a rough total storage request the deployment will
 * place on the namespace ({@code dataSize × members × shards +
 * configSize × 3}) and warns when an existing quota's hard limit
 * on {@code requests.storage} leaves less than that headroom.
 * Absence of quotas → PASS.</p>
 */
public final class QuotaCheck implements PreflightCheck {

    public static final String ID = "preflight.quota";

    @Override public String id() { return ID; }
    @Override public String label() { return "Namespace quota headroom"; }
    @Override public PreflightScope scope(ProvisionModel m) { return PreflightScope.ALWAYS; }

    @Override
    public PreflightResult run(ApiClient client, ProvisionModel m) {
        V1ResourceQuotaList quotas;
        try {
            quotas = new CoreV1Api(client).listNamespacedResourceQuota(m.namespace()).execute();
        } catch (ApiException ae) {
            if (ae.getCode() == 403) {
                return PreflightResult.skipped(ID,
                        "Can't list resourcequotas; skipping headroom check.");
            }
            return PreflightResult.warn(ID,
                    "Quota probe failed: HTTP " + ae.getCode(), null);
        } catch (Exception e) {
            return PreflightResult.warn(ID, "Quota probe errored: " + e.getMessage(), null);
        }

        if (quotas.getItems().isEmpty()) return PreflightResult.pass(ID);

        long wantedGib = estimateStorageGib(m);
        for (V1ResourceQuota q : quotas.getItems()) {
            if (q.getStatus() == null || q.getStatus().getHard() == null) continue;
            var hardStorage = q.getStatus().getHard().get("requests.storage");
            var usedStorage = q.getStatus().getUsed() == null ? null
                    : q.getStatus().getUsed().get("requests.storage");
            if (hardStorage == null) continue;
            long hardGib = toGib(hardStorage.toSuffixedString());
            long usedGib = usedStorage == null ? 0 : toGib(usedStorage.toSuffixedString());
            if (hardGib - usedGib < wantedGib) {
                return PreflightResult.fail(ID,
                        "ResourceQuota '" + nameOf(q) + "' has "
                        + (hardGib - usedGib) + " GiB free; deployment needs ~"
                        + wantedGib + " GiB.",
                        "Raise the quota or downsize storage.");
            }
        }
        return PreflightResult.pass(ID);
    }

    static long estimateStorageGib(ProvisionModel m) {
        int members = m.topology().replicasPerReplset();
        int shards = m.topology().shardCount();
        long dataGib = (long) m.storage().dataSizeGib() * members * shards;
        long cfgGib = m.topology() == Topology.SHARDED
                ? (long) m.storage().configServerSizeGib() * 3
                : 0;
        return dataGib + cfgGib;
    }

    private static String nameOf(V1ResourceQuota q) {
        return q.getMetadata() == null ? "?" : q.getMetadata().getName();
    }

    static long toGib(String q) {
        // Rough Kubernetes quantity → GiB. Handles Mi/Gi/Ti/M/G/T.
        if (q == null) return 0;
        String s = q.trim();
        long mult = 1;
        if (s.endsWith("Ti")) { mult = 1024L; s = s.substring(0, s.length() - 2); }
        else if (s.endsWith("Gi")) { mult = 1L; s = s.substring(0, s.length() - 2); }
        else if (s.endsWith("Mi")) { mult = 0; s = s.substring(0, s.length() - 2); }
        else if (s.endsWith("T")) { mult = 1000L; s = s.substring(0, s.length() - 1); }
        else if (s.endsWith("G")) { mult = 1L; s = s.substring(0, s.length() - 1); }
        else if (s.endsWith("M")) { mult = 0; s = s.substring(0, s.length() - 1); }
        try {
            return Long.parseLong(s.trim()) * mult;
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
