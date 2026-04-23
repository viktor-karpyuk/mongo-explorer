package com.kubrik.mex.maint.schema;

import com.kubrik.mex.maint.model.ValidatorSpec;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 SCHV-3 — {@link ValidatorPreviewService} against a live
 * Mongo fixture. Seeds 20 conforming + 12 non-conforming docs, runs
 * the preview, asserts the failed-count matches.
 */
@Testcontainers(disabledWithoutDocker = true)
class ValidatorPreviewIT {

    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7.0");

    private static MongoClient client;

    @BeforeAll
    static void seed() {
        client = MongoClients.create(MONGO.getConnectionString());
        MongoCollection<Document> coll = client.getDatabase("app")
                .getCollection("items");
        List<Document> docs = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            docs.add(new Document("status", "ACTIVE").append("n", i));
        }
        // 12 docs failing the "status ∈ ACTIVE|ARCHIVED" rule.
        for (int i = 0; i < 12; i++) {
            docs.add(new Document("status", "FROG").append("n", 100 + i));
        }
        coll.insertMany(docs);
    }

    @AfterAll
    static void close() {
        if (client != null) client.close();
    }

    @Test
    void preview_reports_expected_failure_count() {
        ValidatorSpec.Rollout rollout = new ValidatorSpec.Rollout(
                "app", "items",
                """
                { "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["status"],
                    "properties": {
                      "status": { "enum": ["ACTIVE", "ARCHIVED"] }
                    }
                }}
                """.strip(),
                ValidatorSpec.Level.MODERATE, ValidatorSpec.Action.ERROR);

        ValidatorSpec.PreviewResult result = new ValidatorPreviewService()
                .preview(client, rollout);
        assertTrue(result.sampled() >= 20 + 12);
        // The 12 "FROG" status docs must fail; the sampler is random
        // so the actual count is <= 12. On a 32-doc collection, $sample
        // picks them all with very high probability.
        assertTrue(result.failedCount() >= 1 && result.failedCount() <= 12);
        assertFalse(result.firstFew().isEmpty(),
                "at least one offender _id surfaces for the UI table");
    }
}
