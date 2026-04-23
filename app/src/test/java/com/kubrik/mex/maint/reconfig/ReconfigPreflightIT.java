package com.kubrik.mex.maint.reconfig;

import com.kubrik.mex.maint.model.ReconfigSpec;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 RCFG-* — exercises the serializer/preflight against a live
 * one-node replica set. Full 3-node add/remove/vote round-trips live
 * in the 72 h soak rig; single-node cover is enough to validate the
 * wire shape.
 */
@Testcontainers(disabledWithoutDocker = true)
class ReconfigPreflightIT {

    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7.0");

    private static MongoClient client;
    private static final ReconfigSerializer SER = new ReconfigSerializer();
    private static final ReconfigPreflight PRE = new ReconfigPreflight();

    @BeforeAll
    static void open() {
        client = MongoClients.create(MONGO.getConnectionString());
    }

    @AfterAll
    static void close() {
        if (client != null) client.close();
    }

    @Test
    void fromConfigReply_round_trips_live_rs_config() {
        Document reply = client.getDatabase("admin").runCommand(
                new Document("replSetGetConfig", 1));

        Optional<ReconfigSpec.Request> parsed = SER.fromConfigReply(
                reply, new ReconfigSpec.ChangePriority(0, 2));
        assertTrue(parsed.isPresent());
        ReconfigSpec.Request req = parsed.get();
        assertFalse(req.currentMembers().isEmpty());
        assertTrue(req.currentConfigVersion() > 0);

        // A priority bump of the existing member must be clean.
        ReconfigPreflight.Result r = PRE.check(req);
        assertFalse(r.hasBlocking(),
                "priority bump on a live one-node rs must not block");
    }
}
