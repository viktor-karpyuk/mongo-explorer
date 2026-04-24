package com.kubrik.mex.k8s.kind;

import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.client.KubeConfigLoader;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.preflight.checks.ClusterVersionCheck;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.VersionApi;
import io.kubernetes.client.openapi.models.VersionInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.1 Q2.8-N — Blessed-matrix smoke, parametrised across 1.29 / 1.30 / 1.31.
 *
 * <p>The three blessed minors (milestone §7.8) can't all run inside one
 * JVM because kind clusters pin a single control-plane version. The
 * intended workflow is to invoke this IT three times, once per
 * version:</p>
 *
 * <pre>
 *   for v in 1.29.14 1.30.10 1.31.6; do
 *     kind delete cluster --name mex-k8s-it || true
 *     kind create cluster --name mex-k8s-it --image kindest/node:v${v}
 *     MEX_K8S_IT=kind ./gradlew :app:k8sKindTest
 *   done
 * </pre>
 *
 * <p>The test reads the active kind cluster's {@code /version}, reduces
 * it to {@code major.minor}, and asserts it's on {@link
 * ClusterVersionCheck#BLESSED} — so running the suite three times with
 * three kind images exercises the full matrix. Out-of-matrix versions
 * surface a clear failure that points at the blessed set.</p>
 *
 * <p>Skipped unless {@code MEX_K8S_IT=kind}, same as the other kind
 * ITs — no CI impact until kind is wired up in a blessed runner.</p>
 */
@Tag("k8sKind")
@EnabledIfEnvironmentVariable(named = "MEX_K8S_IT", matches = "kind")
class K8sBlessedMatrixIT {

    @Test
    void live_kind_cluster_is_on_blessed_matrix() throws Exception {
        K8sClusterRef ref = resolveKindRef();
        KubeClientFactory factory = new KubeClientFactory();
        ApiClient client = factory.get(ref);
        VersionInfo v = new VersionApi(client).getCode().execute();
        String minor = ClusterVersionCheck.majorDotMinor(v.getGitVersion());
        assertNotNull(minor, "expected parseable gitVersion, got " + v.getGitVersion());

        ClusterVersionCheck.Classification c = ClusterVersionCheck.classify(minor);
        assertEquals(ClusterVersionCheck.Classification.BLESSED, c,
                "kind cluster is running " + v.getGitVersion() + " → minor " + minor
                + ", which is " + c + ". Blessed: " + ClusterVersionCheck.BLESSED
                + ". Re-create kind with one of the blessed kindest/node images.");
    }

    private static K8sClusterRef resolveKindRef() throws Exception {
        List<KubeConfigLoader.DiscoveredContext> all = KubeConfigLoader.discoverAll();
        KubeConfigLoader.DiscoveredContext kind = all.stream()
                .filter(c -> c.summary().contextName().contains("kind"))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "no kind context found — create one with `kind create cluster`."));
        return new K8sClusterRef(
                1L, "kind-matrix-it",
                kind.sourcePath().toAbsolutePath().toString(),
                kind.summary().contextName(),
                kind.summary().namespace(),
                kind.summary().serverUrl(),
                0L, Optional.empty());
    }
}
