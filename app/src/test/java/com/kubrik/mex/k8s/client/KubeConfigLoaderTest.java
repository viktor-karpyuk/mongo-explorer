package com.kubrik.mex.k8s.client;

import com.kubrik.mex.k8s.model.K8sAuthKind;
import com.kubrik.mex.k8s.model.K8sContextSummary;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KubeConfigLoaderTest {

    @Test
    void parses_static_token_user(@TempDir Path dir) throws IOException {
        Path kc = write(dir, """
                apiVersion: v1
                kind: Config
                clusters:
                - name: dev
                  cluster:
                    server: https://dev.example.com:6443
                users:
                - name: alice
                  user:
                    token: aaa.bbb.ccc
                contexts:
                - name: dev-alice
                  context:
                    cluster: dev
                    user: alice
                    namespace: default
                current-context: dev-alice
                """);
        List<K8sContextSummary> ctx = KubeConfigLoader.listContexts(kc);
        assertEquals(1, ctx.size());
        K8sContextSummary c = ctx.get(0);
        assertEquals("dev-alice", c.contextName());
        assertEquals("dev", c.clusterName());
        assertEquals("alice", c.userName());
        assertEquals(K8sAuthKind.TOKEN, c.authKind());
        assertEquals("default", c.namespace().orElse(null));
        assertTrue(c.serverUrl().orElse("").startsWith("https://"));
    }

    @Test
    void exec_plugin_classified_with_binary_hint(@TempDir Path dir) throws IOException {
        Path kc = write(dir, """
                apiVersion: v1
                kind: Config
                clusters:
                - name: eks-prod
                  cluster:
                    server: https://eks.example.com
                users:
                - name: eks-role
                  user:
                    exec:
                      apiVersion: client.authentication.k8s.io/v1beta1
                      command: aws-iam-authenticator
                      args:
                        - token
                        - -i
                        - prod-eks
                contexts:
                - name: prod
                  context:
                    cluster: eks-prod
                    user: eks-role
                """);
        List<K8sContextSummary> ctx = KubeConfigLoader.listContexts(kc);
        assertEquals(1, ctx.size());
        assertEquals(K8sAuthKind.EXEC_PLUGIN, ctx.get(0).authKind());
        assertEquals("aws-iam-authenticator", ctx.get(0).execBinary().orElse(null));
        assertTrue(ctx.get(0).authDetail().orElse("").contains("aws-iam-authenticator"));
    }

    @Test
    void oidc_provider_classified(@TempDir Path dir) throws IOException {
        Path kc = write(dir, """
                apiVersion: v1
                kind: Config
                clusters: []
                users:
                - name: oidc-user
                  user:
                    auth-provider:
                      name: oidc
                      config:
                        idp-issuer-url: https://oidc.example.com
                        client-id: mongo-explorer
                contexts:
                - name: prod-oidc
                  context:
                    cluster: x
                    user: oidc-user
                """);
        List<K8sContextSummary> ctx = KubeConfigLoader.listContexts(kc);
        assertEquals(K8sAuthKind.OIDC, ctx.get(0).authKind());
        assertTrue(ctx.get(0).authDetail().orElse("").contains("oidc"));
    }

    @Test
    void client_cert_classified(@TempDir Path dir) throws IOException {
        Path kc = write(dir, """
                apiVersion: v1
                kind: Config
                clusters: []
                users:
                - name: kind-admin
                  user:
                    client-certificate-data: Zm9v
                    client-key-data: YmFy
                contexts:
                - name: kind
                  context:
                    cluster: kind
                    user: kind-admin
                """);
        assertEquals(K8sAuthKind.CLIENT_CERT,
                KubeConfigLoader.listContexts(kc).get(0).authKind());
    }

    @Test
    void unknown_auth_kind_for_empty_user(@TempDir Path dir) throws IOException {
        Path kc = write(dir, """
                apiVersion: v1
                kind: Config
                clusters: []
                users:
                - name: nobody
                  user: {}
                contexts:
                - name: solo
                  context:
                    cluster: x
                    user: nobody
                """);
        assertEquals(K8sAuthKind.UNKNOWN,
                KubeConfigLoader.listContexts(kc).get(0).authKind());
    }

    @Test
    void multiple_contexts_yield_multiple_summaries(@TempDir Path dir) throws IOException {
        Path kc = write(dir, """
                apiVersion: v1
                kind: Config
                clusters:
                - name: dev
                  cluster:
                    server: https://dev.example.com
                - name: staging
                  cluster:
                    server: https://staging.example.com
                users:
                - name: u
                  user:
                    token: t
                contexts:
                - name: dev-ctx
                  context:
                    cluster: dev
                    user: u
                - name: staging-ctx
                  context:
                    cluster: staging
                    user: u
                    namespace: apps
                """);
        List<K8sContextSummary> ctx = KubeConfigLoader.listContexts(kc);
        assertEquals(2, ctx.size());
        assertEquals("dev-ctx", ctx.get(0).contextName());
        assertEquals("staging-ctx", ctx.get(1).contextName());
        assertEquals("apps", ctx.get(1).namespace().orElse(null));
    }

    @Test
    void unreadable_kubeconfig_throws_io(@TempDir Path dir) {
        Path missing = dir.resolve("not-there.yaml");
        assertThrows(IOException.class, () -> KubeConfigLoader.listContexts(missing));
    }

    private static Path write(Path dir, String body) throws IOException {
        Path p = dir.resolve("kubeconfig.yaml");
        Files.writeString(p, body);
        return p;
    }
}
