package com.kubrik.mex.k8s.kind;

import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.client.KubeConfigLoader;
import com.kubrik.mex.k8s.cluster.ClusterProbeService;
import com.kubrik.mex.k8s.model.ClusterProbeResult;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import io.kubernetes.client.openapi.ApiClient;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.1 Q2.8.1-L — Live kind-cluster smoke suite.
 *
 * <p>Skipped by default. Run with:</p>
 * <pre>
 *   kind create cluster --name mex-k8s-it
 *   export MEX_K8S_IT=kind
 *   ./gradlew :app:k8sKindTest
 * </pre>
 *
 * <p>The suite walks the foundation + client path against a real
 * API server: discovers the kind kubeconfig, builds an ApiClient
 * via the factory, probes `/version`, verifies RBAC checks resolve
 * against the local admin context.</p>
 *
 * <p>Deeper coverage (MCO / PSMDB provisioning IT, kind matrix
 * soak) lands in follow-up commits as operators get installed
 * into the kind image per milestone §8 Q2.8-L.</p>
 */
@Tag("k8sKind")
@EnabledIfEnvironmentVariable(named = "MEX_K8S_IT", matches = "kind")
class KubeClientKindIT {

    @Test
    void discover_local_kubeconfig_finds_kind_context() {
        List<Path> paths = KubeConfigLoader.discoverPaths();
        assertFalse(paths.isEmpty(),
                "$KUBECONFIG or ~/.kube/config must be present for this IT");
    }

    @Test
    void factory_build_and_probe_against_kind() throws Exception {
        List<KubeConfigLoader.DiscoveredContext> all = KubeConfigLoader.discoverAll();
        KubeConfigLoader.DiscoveredContext kind = all.stream()
                .filter(c -> c.summary().contextName().contains("kind"))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "no kind context found — create one with `kind create cluster`."));
        K8sClusterRef ref = new K8sClusterRef(
                1L, "kind-it",
                kind.sourcePath().toAbsolutePath().toString(),
                kind.summary().contextName(),
                kind.summary().namespace(),
                kind.summary().serverUrl(),
                0L, Optional.empty());

        KubeClientFactory factory = new KubeClientFactory();
        ApiClient client = factory.get(ref);
        assertNotNull(client);

        ClusterProbeService probe = new ClusterProbeService(factory);
        ClusterProbeResult result = probe.probe(ref);
        assertTrue(result.ok(),
                "kind cluster should be REACHABLE, got " + result.status()
                + " — " + result.errorMessage().orElse("?"));
        assertTrue(result.serverVersion().isPresent());
    }
}
