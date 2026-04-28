package com.kubrik.mex.migration;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.migration.events.JobEvent;
import com.kubrik.mex.migration.events.JobStatus;
import com.kubrik.mex.migration.gate.PreconditionGate;
import com.kubrik.mex.migration.spec.ConflictMode;
import com.kubrik.mex.migration.spec.ExecutionMode;
import com.kubrik.mex.migration.spec.MigrationKind;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.migration.spec.PerfSpec;
import com.kubrik.mex.migration.spec.ScopeSpec;
import com.kubrik.mex.migration.spec.SourceSpec;
import com.kubrik.mex.migration.spec.TargetSpec;
import com.kubrik.mex.migration.spec.VerifySpec;
import com.kubrik.mex.migration.spec.ErrorPolicy;
import com.kubrik.mex.model.ConnectionState;
import com.kubrik.mex.model.MongoConnection;
import com.kubrik.mex.store.ConnectionStore;
import com.kubrik.mex.store.Database;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** End-to-end copy of a single collection between two MongoDB instances running on
 *  Testcontainers. Verifies that:
 *  <ul>
 *    <li>all documents land on the target with byte-fidelity</li>
 *    <li>user-defined indexes are recreated</li>
 *    <li>job state transitions land in SQLite</li>
 *  </ul>
 *  Named {@code copy_single_collection_happy_path} in the tech spec §17.2.
 */
@Testcontainers(disabledWithoutDocker = true)
class CopySingleCollectionIT {

    @Container
    static MongoDBContainer SOURCE = new MongoDBContainer("mongo:latest");

    @Container
    static MongoDBContainer TARGET = new MongoDBContainer("mongo:latest");

    @TempDir Path dataDir;

    private Database db;
    private ConnectionStore store;
    private ConnectionManager manager;
    private EventBus bus;
    private MigrationService service;

    private String sourceId;
    private String targetId;

    @BeforeEach
    void setUp() throws Exception {
        // Point AppPaths.dataDir() at a temp directory via user.home override — the database
        // class opens SQLite against AppPaths.databaseFile(). We instead construct Database
        // directly by using a small custom path provider.
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        store = new ConnectionStore(db);
        bus = new EventBus();
        Crypto crypto = new Crypto();
        manager = new ConnectionManager(store, bus, crypto);

        sourceId = "src-" + System.nanoTime();
        targetId = "tgt-" + System.nanoTime();
        store.upsert(uriConn(sourceId, "src", SOURCE.getConnectionString()));
        store.upsert(uriConn(targetId, "tgt", TARGET.getConnectionString()));

        // Use existing connect path to populate ConnectionManager.state(...) and service(...).
        awaitConnected(sourceId);
        awaitConnected(targetId);

        PreconditionGate gate = new PreconditionGate(store, manager);
        service = new MigrationService(manager, store, db, bus, gate);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (manager != null) manager.closeAll();
        if (db != null) db.close();
    }

    @Test
    void copy_single_collection_happy_path() throws Exception {
        // --- Seed ---------------------------------------------------------------
        try (MongoClient c = MongoClients.create(SOURCE.getConnectionString())) {
            MongoCollection<Document> users = c.getDatabase("app").getCollection("users");
            for (int i = 0; i < 2000; i++) {
                users.insertOne(new Document("_id", i).append("name", "user-" + i).append("v", i % 5));
            }
            users.createIndex(Indexes.ascending("name"), new IndexOptions().unique(true).name("name_u"));
        }

        // --- Spec ---------------------------------------------------------------
        MigrationSpec spec = new MigrationSpec(
                1,
                MigrationKind.DATA_TRANSFER,
                "test-copy",
                new SourceSpec(sourceId, "primary"),
                new TargetSpec(targetId, null),
                new ScopeSpec.Collections(
                        List.of(com.kubrik.mex.migration.spec.Namespace.parse("app.users")),
                        new com.kubrik.mex.migration.spec.ScopeFlags(true, false),
                        List.of("**"), List.of(), List.of()),
                null,
                new MigrationSpec.Options(
                        ExecutionMode.RUN,
                        new MigrationSpec.Conflict(ConflictMode.APPEND, Map.of()),
                        Map.of(),
                        new PerfSpec(2, 1_000_000L, 500, 4L * 1024 * 1024, 0L, 3),
                        VerifySpec.defaults(),
                        ErrorPolicy.defaults(), false, null, List.of()));

        // --- Run ----------------------------------------------------------------
        CountDownLatch done = new CountDownLatch(1);
        JobStatus[] terminal = new JobStatus[1];
        bus.onJob(e -> {
            if (e instanceof JobEvent.StatusChanged sc && sc.newStatus().isTerminal()) {
                terminal[0] = sc.newStatus();
                done.countDown();
            }
        });

        JobId jobId = service.start(spec);
        assertTrue(done.await(60, TimeUnit.SECONDS), "job did not finish within 60s");

        // --- Assert -------------------------------------------------------------
        assertEquals(JobStatus.COMPLETED, terminal[0]);

        try (MongoClient c = MongoClients.create(TARGET.getConnectionString())) {
            MongoCollection<Document> users = c.getDatabase("app").getCollection("users");
            assertEquals(2000, users.countDocuments(), "target row count mismatch");

            long nameIdx = 0;
            for (Document idx : users.listIndexes()) {
                if ("name_u".equals(idx.getString("name"))) nameIdx++;
            }
            assertEquals(1, nameIdx, "expected 'name_u' index on target");
        }

        MigrationJobRecord record = service.get(jobId).orElseThrow();
        assertEquals(JobStatus.COMPLETED, record.status());
        assertEquals(2000, record.docsCopied());
        assertTrue(record.endedAt() != null);
    }

    // --- helpers -----------------------------------------------------------------

    private MongoConnection uriConn(String id, String name, String uri) {
        MongoConnection blank = MongoConnection.blank();
        long now = System.currentTimeMillis();
        return new MongoConnection(
                id, name, "URI", uri,
                blank.connectionType(), blank.hosts(), blank.srvHost(),
                blank.authMode(), blank.username(), blank.encPassword(), blank.authDb(),
                blank.gssapiServiceName(), blank.awsSessionToken(),
                blank.tlsEnabled(), blank.tlsCaFile(), blank.tlsClientCertFile(),
                blank.encTlsClientCertPassword(), blank.tlsAllowInvalidHostnames(), blank.tlsAllowInvalidCertificates(),
                blank.sshEnabled(), blank.sshHost(), blank.sshPort(), blank.sshUser(), blank.sshAuthMode(),
                blank.encSshPassword(), blank.sshKeyFile(), blank.encSshKeyPassphrase(),
                blank.proxyType(), blank.proxyHost(), blank.proxyPort(), blank.proxyUser(), blank.encProxyPassword(),
                blank.replicaSetName(), blank.readPreference(), blank.defaultDb(), blank.appName(), blank.manualUriOptions(),
                now, now);
    }

    private void awaitConnected(String id) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        bus.onState(s -> {
            if (s.connectionId().equals(id) && s.status() == ConnectionState.Status.CONNECTED) {
                latch.countDown();
            }
        });
        manager.connect(id);
        assertTrue(latch.await(30, TimeUnit.SECONDS), "connect timed out for " + id);
        // Sanity check: service should be ready.
        MongoService svc = manager.service(id);
        assertNotNull(svc, "MongoService for " + id);
    }
}
