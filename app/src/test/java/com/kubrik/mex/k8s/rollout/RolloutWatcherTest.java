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
}
