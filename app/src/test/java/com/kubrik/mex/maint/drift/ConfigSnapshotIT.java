package com.kubrik.mex.maint.drift;

import com.kubrik.mex.maint.model.ConfigSnapshot;
import com.kubrik.mex.store.Database;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 DRIFT-CFG-* — captureAll against a live mongod. Asserts that
 * two captures with no config change produce identical SHA-256s (so
 * the drift engine collapses them) and that mutating a parameter
 * between captures makes the hashes diverge.
 */
@Testcontainers(disabledWithoutDocker = true)
class ConfigSnapshotIT {

    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:latest");

    @TempDir Path dataDir;

    private Database db;
    private MongoClient client;
    private ConfigSnapshotDao dao;
    private ConfigSnapshotService service;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        client = MongoClients.create(MONGO.getConnectionString());
        dao = new ConfigSnapshotDao(db);
        service = new ConfigSnapshotService(dao);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) client.close();
        if (db != null) db.close();
    }

    @Test
    void repeat_capture_yields_matching_parameter_hashes() {
        long now = System.currentTimeMillis();
        List<Long> a = service.captureAll(client, "cx", "primary", now);
        List<Long> b = service.captureAll(client, "cx", "primary", now + 1000);
        assertFalse(a.isEmpty());
        assertFalse(b.isEmpty());

        // Latest two PARAMETERS rows should share a sha256 — the
        // mongod config didn't change between captures.
        var latestParams = dao.listForConnection("cx", 50).stream()
                .filter(r -> r.kind() == ConfigSnapshot.Kind.PARAMETERS)
                .toList();
        assertTrue(latestParams.size() >= 2);
        assertEquals(latestParams.get(0).sha256(),
                latestParams.get(1).sha256(),
                "unchanged config must produce matching hashes");
    }

    @Test
    void changing_a_parameter_diverges_the_hash() {
        long now = System.currentTimeMillis();
        service.captureAll(client, "cx", "primary", now);

        // Mutate a safe parameter between captures.
        client.getDatabase("admin").runCommand(new org.bson.Document(
                "setParameter", 1).append("ttlMonitorSleepSecs", 72));

        service.captureAll(client, "cx", "primary", now + 1000);

        var rows = dao.listForConnection("cx", 50).stream()
                .filter(r -> r.kind() == ConfigSnapshot.Kind.PARAMETERS)
                .toList();
        assertNotEquals(rows.get(0).sha256(), rows.get(1).sha256());

        // Restore.
        client.getDatabase("admin").runCommand(new org.bson.Document(
                "setParameter", 1).append("ttlMonitorSleepSecs", 60));
    }
}
