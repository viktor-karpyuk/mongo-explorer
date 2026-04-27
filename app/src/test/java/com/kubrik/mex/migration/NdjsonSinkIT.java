package com.kubrik.mex.migration;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.migration.events.JobEvent;
import com.kubrik.mex.migration.events.JobStatus;
import com.kubrik.mex.migration.gate.PreconditionGate;
import com.kubrik.mex.migration.spec.ConflictMode;
import com.kubrik.mex.migration.spec.ErrorPolicy;
import com.kubrik.mex.migration.spec.ExecutionMode;
import com.kubrik.mex.migration.spec.MigrationKind;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.migration.spec.Namespace;
import com.kubrik.mex.migration.spec.PerfSpec;
import com.kubrik.mex.migration.spec.ScopeFlags;
import com.kubrik.mex.migration.spec.ScopeSpec;
import com.kubrik.mex.migration.spec.SinkSpec;
import com.kubrik.mex.migration.spec.SourceSpec;
import com.kubrik.mex.migration.spec.TargetSpec;
import com.kubrik.mex.migration.spec.VerifySpec;
import com.kubrik.mex.model.ConnectionState;
import com.kubrik.mex.model.MongoConnection;
import com.kubrik.mex.store.ConnectionStore;
import com.kubrik.mex.store.Database;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** EXT-2 — round-trips a small collection from MongoDB into an NDJSON file sink and verifies:
 *  <ul>
 *    <li>the NDJSON file is created at {@code <path>/<db>.<coll>.ndjson}</li>
 *    <li>line count matches source document count</li>
 *    <li>every emitted line is valid JSON and carries the expected fields</li>
 *    <li>no Mongo target collection is created (sink bypass)</li>
 *    <li>the job reaches {@code COMPLETED} and {@code docsCopied} matches</li>
 *  </ul> */
@Testcontainers(disabledWithoutDocker = true)
class NdjsonSinkIT {

    @Container
    static MongoDBContainer SOURCE = new MongoDBContainer("mongo:latest");

    @Container
    static MongoDBContainer TARGET = new MongoDBContainer("mongo:latest");

    @TempDir Path dataDir;
    @TempDir Path sinkDir;

    private Database db;
    private ConnectionStore store;
    private ConnectionManager manager;
    private EventBus bus;
    private MigrationService service;

    private String sourceId;
    private String targetId;

    @BeforeEach
    void setUp() throws Exception {
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

        awaitConnected(sourceId);
        awaitConnected(targetId);

        service = new MigrationService(manager, store, db, bus,
                new PreconditionGate(store, manager));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (manager != null) manager.closeAll();
        if (db != null) db.close();
    }

    @Test
    void exports_collection_as_ndjson() throws Exception {
        // --- Seed source --------------------------------------------------------
        final int total = 50;
        try (MongoClient c = MongoClients.create(SOURCE.getConnectionString())) {
            MongoCollection<Document> users = c.getDatabase("app").getCollection("users");
            for (int i = 0; i < total; i++) {
                users.insertOne(new Document("_id", i)
                        .append("name", "user-" + i)
                        .append("v", i % 5));
            }
        }

        // --- Spec with NDJSON sink ----------------------------------------------
        MigrationSpec spec = new MigrationSpec(
                1,
                MigrationKind.DATA_TRANSFER,
                "ndjson-export",
                new SourceSpec(sourceId, "primary"),
                new TargetSpec(targetId, null),
                new ScopeSpec.Collections(
                        List.of(Namespace.parse("app.users")),
                        new ScopeFlags(false, false),
                        List.of("**"), List.of(), List.of()),
                null,
                new MigrationSpec.Options(
                        ExecutionMode.RUN,
                        new MigrationSpec.Conflict(ConflictMode.APPEND, Map.of()),
                        Map.of(),
                        PerfSpec.defaults(),
                        VerifySpec.defaults(),
                        ErrorPolicy.defaults(), false, null,
                        List.of(new SinkSpec(SinkSpec.SinkKind.NDJSON, sinkDir.toString()))));

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
        assertTrue(done.await(60, TimeUnit.SECONDS), "sink export did not finish within 60s");
        assertEquals(JobStatus.COMPLETED, terminal[0]);

        // --- File exists with the expected line count ---------------------------
        Path outputFile = sinkDir.resolve("app.users.ndjson");
        assertTrue(Files.exists(outputFile), "NDJSON output file should exist at " + outputFile);
        List<String> lines = Files.readAllLines(outputFile);
        assertEquals(total, lines.size(), "one line per document");

        // --- Every line parses + first line carries the expected shape ----------
        for (String line : lines) {
            assertDoesNotThrow(() -> Document.parse(line),
                    "every line must be valid JSON: " + line);
        }
        Document first = Document.parse(lines.get(0));
        assertNotNull(first.get("_id"));
        assertTrue(first.getString("name").startsWith("user-"));

        // --- Sink bypass — target Mongo must not have the collection ------------
        try (MongoClient c = MongoClients.create(TARGET.getConnectionString())) {
            boolean hasUsers = false;
            for (String n : c.getDatabase("app").listCollectionNames()) {
                if ("users".equals(n)) hasUsers = true;
            }
            assertFalse(hasUsers,
                    "sink path must not write to the MongoDB target (no app.users on target)");
        }

        // --- Job record reflects the right doc count ----------------------------
        MigrationJobRecord record = service.get(jobId).orElseThrow();
        assertEquals(total, record.docsCopied());
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

    private void awaitConnected(String id) throws Exception {
        manager.connect(id);
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (manager.state(id).status() == ConnectionState.Status.CONNECTED) return;
            Thread.sleep(100);
        }
        throw new AssertionError("connection " + id + " never reached CONNECTED");
    }
}
