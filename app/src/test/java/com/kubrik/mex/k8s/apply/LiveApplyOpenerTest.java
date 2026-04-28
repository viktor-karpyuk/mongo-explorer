package com.kubrik.mex.k8s.apply;

import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class LiveApplyOpenerTest {

    @Test
    void split_group_and_version_core() {
        assertEquals("", LiveApplyOpener.splitGroup("v1"));
        assertEquals("v1", LiveApplyOpener.splitVersion("v1"));
    }

    @Test
    void split_group_and_version_apps() {
        assertEquals("apps", LiveApplyOpener.splitGroup("apps/v1"));
        assertEquals("v1", LiveApplyOpener.splitVersion("apps/v1"));
    }

    @Test
    void split_group_and_version_crd() {
        assertEquals("psmdb.percona.com",
                LiveApplyOpener.splitGroup("psmdb.percona.com/v1"));
        assertEquals("v1",
                LiveApplyOpener.splitVersion("psmdb.percona.com/v1"));
    }

    @Test
    void plural_table_handles_every_rendered_kind() {
        assertEquals("secrets", LiveApplyOpener.pluralFor("Secret"));
        assertEquals("configmaps", LiveApplyOpener.pluralFor("ConfigMap"));
        assertEquals("poddisruptionbudgets", LiveApplyOpener.pluralFor("PodDisruptionBudget"));
        assertEquals("servicemonitors", LiveApplyOpener.pluralFor("ServiceMonitor"));
        assertEquals("cronjobs", LiveApplyOpener.pluralFor("CronJob"));
        assertEquals("mongodbcommunity", LiveApplyOpener.pluralFor("MongoDBCommunity"));
        assertEquals("perconaservermongodbs", LiveApplyOpener.pluralFor("PerconaServerMongoDB"));
    }

    @Test
    void plural_fallback_for_unknown_kind() {
        assertEquals("foobars", LiveApplyOpener.pluralFor("Foobar"));
    }

    @Test
    void to_dynamic_round_trips_yaml_through_gson() throws IOException {
        String yaml = """
                apiVersion: v1
                kind: Secret
                metadata:
                  name: my-secret
                  namespace: mongo
                type: Opaque
                stringData:
                  password: "redacted"
                """;
        DynamicKubernetesObject obj = LiveApplyOpener.toDynamic(yaml);
        assertEquals("v1", obj.getApiVersion());
        assertEquals("Secret", obj.getKind());
        assertNotNull(obj.getMetadata());
        assertEquals("my-secret", obj.getMetadata().getName());
        assertEquals("mongo", obj.getMetadata().getNamespace());
        // Raw JSON must carry the stringData sub-object for create() to
        // send the right payload.
        assertTrue(obj.getRaw().has("stringData"));
        assertEquals("redacted",
                obj.getRaw().getAsJsonObject("stringData").get("password").getAsString());
    }

    @Test
    void to_dynamic_handles_crd_yaml() throws IOException {
        String yaml = """
                apiVersion: psmdb.percona.com/v1
                kind: PerconaServerMongoDB
                metadata:
                  name: prod-cluster
                  namespace: mongo
                spec:
                  image: percona/percona-server-mongodb:7.0.8-5
                  replsets:
                  - name: rs0
                    size: 3
                """;
        DynamicKubernetesObject obj = LiveApplyOpener.toDynamic(yaml);
        assertEquals("psmdb.percona.com/v1", obj.getApiVersion());
        assertEquals("PerconaServerMongoDB", obj.getKind());
        assertTrue(obj.getRaw().getAsJsonObject("spec").has("replsets"));
    }

    @Test
    void to_dynamic_rejects_non_map_yaml() {
        // A bare list at the root isn't a Kubernetes object.
        assertThrows(Exception.class, () ->
                LiveApplyOpener.toDynamic("- one\n- two\n"));
    }
}
