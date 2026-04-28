package com.kubrik.mex.k8s.adversarial;

import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.client.KubeConfigLoader;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.1 Q2.8.1-L — Adversarial: malformed kubeconfigs must surface
 * an {@link IOException} with a recognisable message, never crash
 * the JVM or leak a half-built {@code ApiClient}.
 */
class MalformedKubeconfigTest {

    @TempDir Path tmp;

    @Test
    void loader_on_broken_yaml_bubbles_io_exception() throws Exception {
        Path kc = tmp.resolve("broken.yaml");
        Files.writeString(kc, ": : : this :: is :: not :: yaml\n");
        IOException io = assertThrows(IOException.class,
                () -> KubeConfigLoader.listContexts(kc));
        assertNotNull(io.getMessage());
    }

    @Test
    void factory_on_missing_context_throws_descriptive() {
        Path kc = tmp.resolve("valid-but-no-such-context.yaml");
        try {
            Files.writeString(kc, """
                    apiVersion: v1
                    kind: Config
                    clusters: []
                    users: []
                    contexts: []
                    """);
        } catch (IOException e) { fail(e); }
        K8sClusterRef ref = new K8sClusterRef(1L, "t",
                kc.toAbsolutePath().toString(),
                "definitely-not-present",
                Optional.empty(), Optional.empty(), 0L, Optional.empty());
        IOException io = assertThrows(IOException.class,
                () -> new KubeClientFactory().get(ref));
        assertTrue(io.getMessage().contains("context"),
                "error should identify the missing context: " + io.getMessage());
    }

    @Test
    void factory_on_nonexistent_path_throws_descriptive() {
        K8sClusterRef ref = new K8sClusterRef(1L, "t",
                "/nonexistent/path/kube.yaml", "ctx",
                Optional.empty(), Optional.empty(), 0L, Optional.empty());
        IOException io = assertThrows(IOException.class,
                () -> new KubeClientFactory().get(ref));
        assertTrue(io.getMessage().contains("not found") || io.getMessage().contains("kubeconfig"),
                "error should identify the missing kubeconfig: " + io.getMessage());
    }

    @Test
    void empty_yaml_file_yields_empty_context_list() throws Exception {
        Path kc = tmp.resolve("empty.yaml");
        Files.writeString(kc, "");
        assertTrue(KubeConfigLoader.listContexts(kc).isEmpty(),
                "empty file shouldn't crash — just return no contexts");
    }
}
