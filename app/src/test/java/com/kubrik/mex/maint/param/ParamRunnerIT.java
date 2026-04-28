package com.kubrik.mex.maint.param;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 PARAM-* — {@link ParamRunner} get + set against a live mongod.
 * Exercises a safe, reversible parameter so the IT can run repeatedly.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParamRunnerIT {

    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:latest");

    private static MongoClient client;
    private static final ParamRunner RUNNER = new ParamRunner();

    @BeforeAll
    static void open() {
        client = MongoClients.create(MONGO.getConnectionString());
    }

    @AfterAll
    static void close() {
        if (client != null) client.close();
    }

    @Test
    void get_reads_a_known_parameter() {
        // ttlMonitorSleepSecs exists on every supported version.
        Optional<Object> v = RUNNER.get(client, "ttlMonitorSleepSecs");
        assertTrue(v.isPresent());
    }

    @Test
    void get_unknown_parameter_returns_empty_not_throw() {
        assertTrue(RUNNER.get(client, "notAParamNameEverFound").isEmpty());
    }

    @Test
    void set_round_trips_a_numeric_parameter() {
        Optional<Object> before = RUNNER.get(client, "ttlMonitorSleepSecs");
        assertTrue(before.isPresent());
        int original = ((Number) before.get()).intValue();

        ParamRunner.Outcome outcome = RUNNER.set(client,
                "ttlMonitorSleepSecs", original + 1);
        assertInstanceOf(ParamRunner.Outcome.Ok.class, outcome);

        // Verify the update landed.
        Optional<Object> after = RUNNER.get(client, "ttlMonitorSleepSecs");
        assertEquals(original + 1, ((Number) after.get()).intValue());

        // Restore for the next run.
        RUNNER.set(client, "ttlMonitorSleepSecs", original);
    }
}
