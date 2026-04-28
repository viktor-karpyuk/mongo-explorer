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
import com.kubrik.mex.migration.spec.PerfSpec;
import com.kubrik.mex.migration.spec.ScopeSpec;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** End-to-end throughput harness. Off by default — enable with
 *  {@code -Dmex.perf=true}. Performance targets from {@code docs/milestone-mvp.md §3.11}
 *  are aspirational for bare-metal localhost; this Testcontainers-backed test sets a
 *  conservative floor of 2 000 docs/sec that still catches major regressions. */
@Tag("perf")
@Testcontainers(disabledWithoutDocker = true)
@EnabledIfSystemProperty(named = "mex.perf", matches = "true")
class ThroughputPerfTest {

    @Container static MongoDBContainer SOURCE = new MongoDBContainer("mongo:latest");
    @Container static MongoDBContainer TARGET = new MongoDBContainer("mongo:latest");

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
        service = new MigrationService(manager, store, db, bus, new PreconditionGate(store, manager));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (manager != null) manager.closeAll();
        if (db != null) db.close();
    }

    @Test
    void steady_state_throughput_single_collection() throws Exception {
        final int total = 50_000;
        seed(total);

        MigrationSpec spec = new MigrationSpec(
                1, MigrationKind.DATA_TRANSFER, "perf-single",
                new SourceSpec(sourceId, "primary"),
                new TargetSpec(targetId, null),
                new ScopeSpec.Collections(
                        List.of(com.kubrik.mex.migration.spec.Namespace.parse("perf.events")),
                        new com.kubrik.mex.migration.spec.ScopeFlags(false, false),
                        List.of("**"), List.of(), List.of()),
                null,
                new MigrationSpec.Options(
                        ExecutionMode.RUN,
                        new MigrationSpec.Conflict(ConflictMode.APPEND, Map.of()),
                        Map.of(),
                        new PerfSpec(1, 10_000_000L, 1_000, 16L * 1024 * 1024, 0L, 3),
                        new VerifySpec(false, 0, false),
                        ErrorPolicy.defaults(), false, null, List.of()));

        long t0 = System.nanoTime();
        awaitCompletion(service.start(spec));
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
        double docsPerSec = total * 1000.0 / elapsedMs;

        System.out.printf("[perf] single: %,d docs in %,d ms → %,.0f docs/s%n",
                total, elapsedMs, docsPerSec);
        assertTrue(docsPerSec >= 2_000,
                "single-collection throughput " + (int) docsPerSec + " docs/s < 2 000 floor");
    }

    @Test
    void steady_state_throughput_four_parallel_collections() throws Exception {
        final int perColl = 12_000;
        final int collections = 4;
        seedMany(collections, perColl);

        List<com.kubrik.mex.migration.spec.Namespace> namespaces = new ArrayList<>();
        for (int i = 0; i < collections; i++) {
            namespaces.add(com.kubrik.mex.migration.spec.Namespace.parse("perf.stream_" + i));
        }

        MigrationSpec spec = new MigrationSpec(
                1, MigrationKind.DATA_TRANSFER, "perf-parallel",
                new SourceSpec(sourceId, "primary"),
                new TargetSpec(targetId, null),
                new ScopeSpec.Collections(namespaces,
                        new com.kubrik.mex.migration.spec.ScopeFlags(false, false),
                        List.of("**"), List.of(), List.of()),
                null,
                new MigrationSpec.Options(
                        ExecutionMode.RUN,
                        new MigrationSpec.Conflict(ConflictMode.APPEND, Map.of()),
                        Map.of(),
                        new PerfSpec(4, 10_000_000L, 1_000, 16L * 1024 * 1024, 0L, 3),
                        new VerifySpec(false, 0, false),
                        ErrorPolicy.defaults(), false, null, List.of()));

        long t0 = System.nanoTime();
        awaitCompletion(service.start(spec));
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
        int totalDocs = perColl * collections;
        double docsPerSec = totalDocs * 1000.0 / elapsedMs;

        System.out.printf("[perf] 4×parallel: %,d docs in %,d ms → %,.0f docs/s%n",
                totalDocs, elapsedMs, docsPerSec);
        assertTrue(docsPerSec >= 4_000,
                "4-parallel throughput " + (int) docsPerSec + " docs/s < 4 000 floor");
    }

    // --- helpers -----------------------------------------------------------------

    private void awaitCompletion(JobId id) throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);
        JobStatus[] terminal = new JobStatus[1];
        bus.onJob(e -> {
            if (e instanceof JobEvent.StatusChanged sc
                    && sc.jobId().equals(id)
                    && sc.newStatus().isTerminal()) {
                terminal[0] = sc.newStatus();
                done.countDown();
            }
        });
        assertTrue(done.await(5, TimeUnit.MINUTES), "perf job did not finish in 5 min");
        assertEquals(JobStatus.COMPLETED, terminal[0]);
    }

    private void seed(int n) {
        try (MongoClient c = MongoClients.create(SOURCE.getConnectionString())) {
            c.getDatabase("perf").getCollection("events").drop();
            MongoCollection<Document> coll = c.getDatabase("perf").getCollection("events");
            List<Document> batch = new ArrayList<>(1000);
            for (int i = 0; i < n; i++) {
                batch.add(new Document("_id", i).append("payload", "x".repeat(800)));
                if (batch.size() == 1000) { coll.insertMany(batch); batch.clear(); }
            }
            if (!batch.isEmpty()) coll.insertMany(batch);
        }
        try (MongoClient c = MongoClients.create(TARGET.getConnectionString())) {
            c.getDatabase("perf").getCollection("events").drop();
        }
    }

    private void seedMany(int count, int perColl) {
        try (MongoClient c = MongoClients.create(SOURCE.getConnectionString())) {
            for (int k = 0; k < count; k++) {
                String name = "stream_" + k;
                c.getDatabase("perf").getCollection(name).drop();
                MongoCollection<Document> coll = c.getDatabase("perf").getCollection(name);
                List<Document> batch = new ArrayList<>(1000);
                for (int i = 0; i < perColl; i++) {
                    batch.add(new Document("_id", i).append("payload", "x".repeat(800)));
                    if (batch.size() == 1000) { coll.insertMany(batch); batch.clear(); }
                }
                if (!batch.isEmpty()) coll.insertMany(batch);
            }
        }
        try (MongoClient c = MongoClients.create(TARGET.getConnectionString())) {
            for (int k = 0; k < count; k++) c.getDatabase("perf").getCollection("stream_" + k).drop();
        }
    }

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
        assertTrue(latch.await(30, TimeUnit.SECONDS), "connect timed out");
        MongoService svc = manager.service(id);
        assertNotNull(svc);
    }
}
