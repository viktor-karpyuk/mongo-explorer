package com.kubrik.mex.k8s.discovery;

import com.kubrik.mex.k8s.model.DiscoveredMongo;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1StatefulSetSpec;
import io.kubernetes.client.openapi.models.V1StatefulSetStatus;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PlainStsDiscovererTest {

    @Test
    void plain_mongo_sts_is_identified() {
        V1StatefulSet sts = sts("my-mongo", "default", 3,
                c("mongo", "mongo:7.0.9"));
        DiscoveredMongo d = PlainStsDiscoverer.tryParse(sts, 99L);
        assertNotNull(d);
        assertEquals(DiscoveredMongo.Origin.PLAIN_STS, d.origin());
        assertEquals(DiscoveredMongo.Topology.RS3, d.topology());
        assertEquals("7.0.9", d.mongoVersion().orElse(null));
    }

    @Test
    void non_mongo_sts_is_skipped() {
        V1StatefulSet sts = sts("nginx", "web", 3, c("nginx", "nginx:1.25"));
        assertNull(PlainStsDiscoverer.tryParse(sts, 1L));
    }

    @Test
    void operator_owned_sts_is_skipped() {
        V1StatefulSet sts = sts("mongo-0", "mongo", 3, c("mongo", "mongo:7.0.9"));
        V1OwnerReference owner = new V1OwnerReference()
                .apiVersion("mongodbcommunity.mongodb.com/v1")
                .kind("MongoDBCommunity")
                .name("mongo-0")
                .controller(true);
        sts.getMetadata().ownerReferences(List.of(owner));
        assertNull(PlainStsDiscoverer.tryParse(sts, 1L),
                "operator-owned STS should be filtered out");
    }

    @Test
    void labels_managed_by_mongodb_operator_filtered_out() {
        V1StatefulSet sts = sts("mongo", "ns", 3, c("mongo", "mongo:7.0.9"));
        sts.getMetadata().labels(Map.of("app.kubernetes.io/managed-by",
                "mongodb-kubernetes-operator"));
        assertNull(PlainStsDiscoverer.tryParse(sts, 1L));
    }

    @Test
    void ready_flag_reflects_replica_agreement() {
        V1StatefulSet sts = sts("rs3", "db", 3, c("mongo", "mongo:7.0.9"));
        sts.status(new V1StatefulSetStatus().replicas(3).readyReplicas(3));
        assertEquals(Boolean.TRUE,
                PlainStsDiscoverer.tryParse(sts, 1L).ready().orElse(null));

        V1StatefulSet partial = sts("rs3", "db", 3, c("mongo", "mongo:7.0.9"));
        partial.status(new V1StatefulSetStatus().replicas(3).readyReplicas(2));
        assertEquals(Boolean.FALSE,
                PlainStsDiscoverer.tryParse(partial, 1L).ready().orElse(null));
    }

    @Test
    void image_tag_extraction_is_lenient() {
        assertEquals("7.0", PlainStsDiscoverer.imageTag("mongo:7.0"));
        assertNull(PlainStsDiscoverer.imageTag("mongo"));
        assertNull(PlainStsDiscoverer.imageTag(null));
    }

    private static V1StatefulSet sts(String name, String ns, int replicas, V1Container container) {
        V1StatefulSet s = new V1StatefulSet();
        s.metadata(new V1ObjectMeta().name(name).namespace(ns));
        s.spec(new V1StatefulSetSpec()
                .replicas(replicas)
                .serviceName(name)
                .template(new V1PodTemplateSpec()
                        .spec(new V1PodSpec().containers(List.of(container)))));
        return s;
    }

    private static V1Container c(String name, String image) {
        return new V1Container().name(name).image(image);
    }
}
