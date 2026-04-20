package com.kubrik.mex.cluster;

import com.kubrik.mex.core.MongoService;
import com.mongodb.client.MongoClient;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.4 Q2.4-A follow-up — input-validation coverage for {@link
 * MongoService#openPeerClient}. Creating a live peer client against the
 * Testcontainers replset isn't round-trippable (the replset advertises the
 * container's internal Docker hostname, which isn't resolvable from the host
 * test runner); the happy path is exercised end-to-end by
 * {@code TopologyServiceIT} when it runs against a sharded fixture.
 */
@Testcontainers(disabledWithoutDocker = true)
class OpenPeerClientTest {

    @Container
    static MongoDBContainer MONGO = new MongoDBContainer("mongo:7.0");

    @Test
    void openPeerClient_rejects_malformed_specs() {
        try (MongoService svc = new MongoService(MONGO.getConnectionString())) {
            assertThrows(IllegalArgumentException.class,
                    () -> svc.openPeerClient("", 1_000));
            assertThrows(IllegalArgumentException.class,
                    () -> svc.openPeerClient(null, 1_000));
            assertThrows(IllegalArgumentException.class,
                    () -> svc.openPeerClient("rs0/", 1_000),
                    "rsName with no seeds must be rejected");
        }
    }

    @Test
    void openPeerClient_parses_rsName_prefixed_spec() {
        // Parser accepts "rsName/h:p" but may timeout trying to reach the peer;
        // we only care that parsing succeeds (creation doesn't throw) and close
        // is clean. The spec targets a loopback port that's closed, so server
        // selection will time out when exercised — but we never exercise it.
        try (MongoService svc = new MongoService(MONGO.getConnectionString())) {
            MongoClient peer = svc.openPeerClient("rs0/127.0.0.1:1", 100);
            assertNotNull(peer);
            peer.close();
        }
    }
}
