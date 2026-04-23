package com.kubrik.mex.k8s.kind;

import com.kubrik.mex.k8s.apply.LiveApplyOpener;
import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.client.KubeConfigLoader;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.rollout.ResourceCatalogue;
import io.kubernetes.client.openapi.ApiClient;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.Optional;
import java.util.UUID;

/**
 * v2.8.1 Q2.8.1-L — Smokes {@link LiveApplyOpener} against a real
 * kind cluster. Creates + deletes a namespace-scoped Secret with a
 * unique name so repeated runs don't collide.
 *
 * <p>Skipped by default. Run with:</p>
 * <pre>
 *   kind create cluster --name mex-k8s-it
 *   export MEX_K8S_IT=kind
 *   ./gradlew :app:k8sKindTest
 * </pre>
 *
 * <p>Catches wiring regressions the pure-Java tests can't — wrong
 * apiVersion splits, wrong plural lookups, mis-serialised YAML, etc.
 * — without needing MCO or PSMDB pre-installed on the cluster.</p>
 */
@Tag("k8sKind")
@EnabledIfEnvironmentVariable(named = "MEX_K8S_IT", matches = "kind")
class LiveApplyKindIT {

    @Test
    void apply_and_delete_a_core_secret() throws Exception {
        K8sClusterRef ref = kindRef();
        ApiClient client = new KubeClientFactory().get(ref);
        LiveApplyOpener opener = new LiveApplyOpener(client);

        String name = "mex-apply-it-" + UUID.randomUUID().toString().substring(0, 8);
        String ns = "default";
        String yaml = """
                apiVersion: v1
                kind: Secret
                metadata:
                  name: %s
                  namespace: %s
                  labels:
                    mex.provisioning/renderer: mongo-explorer-it
                type: Opaque
                stringData:
                  password: "apply-kind-it"
                """.formatted(name, ns);
        ResourceCatalogue.Ref refDoc = new ResourceCatalogue.Ref("v1", "Secret", ns, name);

        try {
            opener.apply(client, refDoc, yaml);
            // Second apply = update path. Must succeed without 409 blowup.
            opener.apply(client, refDoc, yaml);
        } finally {
            // Always tear down so the cluster stays clean across runs.
            opener.delete(client, refDoc);
            // Delete of an already-absent secret must succeed (404 → noop).
            opener.delete(client, refDoc);
        }
    }

    private static K8sClusterRef kindRef() throws Exception {
        var all = KubeConfigLoader.discoverAll();
        var kind = all.stream()
                .filter(c -> c.summary().contextName().contains("kind"))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "no kind context found — create one with `kind create cluster`."));
        return new K8sClusterRef(1L, "kind-it",
                kind.sourcePath().toAbsolutePath().toString(),
                kind.summary().contextName(),
                kind.summary().namespace(),
                kind.summary().serverUrl(),
                0L, Optional.empty());
    }
}
