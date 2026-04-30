package com.kubrik.mex.k8s.rollout;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RolloutWatcherTest {

    @Test
    void plural_for_mco_and_psmdb_matches_renderer_tables() {
        assertEquals("mongodbcommunity", RolloutWatcher.pluralFor("MongoDBCommunity"));
        assertEquals("perconaservermongodbs", RolloutWatcher.pluralFor("PerconaServerMongoDB"));
    }

    @Test
    void plural_fallback_for_unknown_kind() {
        assertEquals("foobars", RolloutWatcher.pluralFor("Foobar"));
    }

    @Test
    void constructor_rejects_non_positive_poll_interval() {
        assertThrows(IllegalArgumentException.class, () ->
                new RolloutWatcher(new com.kubrik.mex.k8s.client.KubeClientFactory(),
                        0, 1000));
    }

    @Test
    void constructor_rejects_non_positive_timeout() {
        assertThrows(IllegalArgumentException.class, () ->
                new RolloutWatcher(new com.kubrik.mex.k8s.client.KubeClientFactory(),
                        1000, 0));
    }

    @Test
    void watch_result_ok_true_only_for_ready() {
        assertTrue(new RolloutWatcher.WatchResult(
                com.kubrik.mex.k8s.operator.DeploymentStatus.READY, 1, "ok").ok());
        assertFalse(new RolloutWatcher.WatchResult(
                com.kubrik.mex.k8s.operator.DeploymentStatus.FAILED, 1, "no").ok());
        assertFalse(new RolloutWatcher.WatchResult(
                com.kubrik.mex.k8s.operator.DeploymentStatus.APPLYING, 1, "no").ok());
        assertFalse(new RolloutWatcher.WatchResult(
                com.kubrik.mex.k8s.operator.DeploymentStatus.UNKNOWN, 1, "no").ok());
    }

    /** When the kubeconfig fetch fails the loop exits before the first
     *  poll — but the no-extension overload still has to delegate to the
     *  7-arg form without NPE. Lock that the bridging path is safe. */
    @Test
    void no_extension_overload_delegates_without_npe() {
        RolloutWatcher w = new RolloutWatcher(
                new com.kubrik.mex.k8s.client.KubeClientFactory() {
                    @Override
                    public io.kubernetes.client.openapi.ApiClient get(
                            com.kubrik.mex.k8s.model.K8sClusterRef ref) throws java.io.IOException {
                        throw new java.io.IOException("no kubeconfig in unit test");
                    }
                }, 100, 200);

        var emitted = new java.util.ArrayList<RolloutEvent>();
        var ref = new com.kubrik.mex.k8s.model.K8sClusterRef(
                7L, "display", "/tmp/none", "ctx",
                java.util.Optional.empty(), java.util.Optional.empty(),
                0L, java.util.Optional.empty());

        // The 6-arg form must call into the 7-arg form with extension=null
        // and surface the kubeconfig error as a single ERROR event.
        var result = w.watch(ref,
                new com.kubrik.mex.k8s.operator.OperatorAdapter() {
                    @Override public com.kubrik.mex.k8s.provision.OperatorId id() {
                        return com.kubrik.mex.k8s.provision.OperatorId.MCO;
                    }
                    @Override public java.util.Set<com.kubrik.mex.k8s.operator.Capability> capabilities() {
                        return java.util.Set.of();
                    }
                    @Override public String crdGroup() { return "x"; }
                    @Override public String crdKind()  { return "X"; }
                    @Override public com.kubrik.mex.k8s.operator.KubernetesManifests render(
                            com.kubrik.mex.k8s.provision.ProvisionModel m) { return null; }
                    @Override public com.kubrik.mex.k8s.operator.DeploymentStatus parseStatus(
                            java.util.Map<String,Object> s,
                            java.util.List<java.util.Map<String,Object>> p,
                            java.util.List<java.util.Map<String,Object>> e) {
                        return com.kubrik.mex.k8s.operator.DeploymentStatus.UNKNOWN;
                    }
                },
                "ns", "name", 42L, emitted::add);

        assertEquals(com.kubrik.mex.k8s.operator.DeploymentStatus.FAILED, result.status());
        assertEquals(1, emitted.size());
        assertEquals(RolloutEvent.Severity.ERROR, emitted.get(0).severity());
    }
}
