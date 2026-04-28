package com.kubrik.mex.labs.k8s.ui;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.1 Q2.8-N7 — filterKubeconfig carves a standalone config
 * out of the merged kubeconfig. Tests pin the structural guarantees
 * the export action relies on.
 */
class LabK8sPaneKubeconfigFilterTest {

    @Test
    void keeps_only_the_chosen_context_plus_referenced_cluster_user() {
        Map<String, Object> root = Map.of(
                "apiVersion", "v1",
                "kind", "Config",
                "current-context", "prod",
                "contexts", List.of(
                        Map.of("name", "dev",
                                "context", Map.of("cluster", "c1", "user", "u1")),
                        Map.of("name", "lab",
                                "context", Map.of("cluster", "c2", "user", "u2"))),
                "clusters", List.of(
                        Map.of("name", "c1", "cluster", Map.of("server", "https://a")),
                        Map.of("name", "c2", "cluster", Map.of("server", "https://b"))),
                "users", List.of(
                        Map.of("name", "u1", "user", Map.of("token", "x")),
                        Map.of("name", "u2", "user", Map.of("token", "y"))));

        Map<String, Object> out = LabK8sPane.filterKubeconfig(root, "lab");

        assertEquals("lab", out.get("current-context"));
        assertEquals(1, ((List<?>) out.get("contexts")).size());
        assertEquals(1, ((List<?>) out.get("clusters")).size());
        assertEquals(1, ((List<?>) out.get("users")).size());

        Map<?, ?> keptCluster = (Map<?, ?>) ((List<?>) out.get("clusters")).get(0);
        assertEquals("c2", keptCluster.get("name"));
        Map<?, ?> keptUser = (Map<?, ?>) ((List<?>) out.get("users")).get(0);
        assertEquals("u2", keptUser.get("name"));
    }

    @Test
    void throws_when_context_name_not_in_kubeconfig() {
        Map<String, Object> root = Map.of(
                "contexts", List.of(Map.of("name", "only", "context", Map.of())));
        assertThrows(IllegalStateException.class,
                () -> LabK8sPane.filterKubeconfig(root, "other"));
    }

    @Test
    void missing_referenced_cluster_or_user_yields_empty_list() {
        // Context references a cluster/user that isn't defined — result
        // has an empty list rather than throwing, so a kubeconfig with
        // an incomplete user stanza can still be exported for inspection.
        Map<String, Object> root = Map.of(
                "contexts", List.of(Map.of("name", "x",
                        "context", Map.of("cluster", "missing", "user", "missing"))),
                "clusters", List.of(),
                "users", List.of());
        Map<String, Object> out = LabK8sPane.filterKubeconfig(root, "x");
        assertTrue(((List<?>) out.get("clusters")).isEmpty());
        assertTrue(((List<?>) out.get("users")).isEmpty());
    }

    @Test
    void default_api_version_and_kind_when_absent() {
        Map<String, Object> root = Map.of(
                "contexts", List.of(Map.of("name", "x", "context", Map.of())));
        Map<String, Object> out = LabK8sPane.filterKubeconfig(root, "x");
        assertEquals("v1", out.get("apiVersion"));
        assertEquals("Config", out.get("kind"));
    }
}
