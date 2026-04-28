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
import org.junit.jupiter.api.Test;
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

/** OBS-6 P2.7: asserts that a running job emits {@code JobEvent.Progress} at the 200 ms
 *  target cadence — ≥ 4 events per wall-clock second sustained over the copy window.
 *  Also validates the OBS-5 RUN-mode invariant that {@code docsCopied == docsProcessed}. */
@Testcontainers(disabledWithoutDocker = true)
class ProgressCadenceIT {

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
        manager = new ConnectionManager(store, bus, new Crypto());
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
    void publishes_at_least_four_progress_events_per_second_sustained() throws Exception {
        // Seed enough docs to keep the pipeline busy for several seconds so the cadence window
        // has statistical meaning. Each doc carries ~1 KB of payload so local Docker mongo
        // doesn't finish in under a tick.
        final int DOC_COUNT = 50_000;
        final String PAYLOAD = "x".repeat(1024);
        try (MongoClient c = MongoClients.create(SOURCE.getConnectionString())) {
            MongoCollection<Document> users = c.getDatabase("app").getCollection("users");
            List<Document> batch = new ArrayList<>(2000);
            for (int i = 0; i < DOC_COUNT; i++) {
                batch.add(new Document("_id", i).append("name", "user-" + i).append("payload", PAYLOAD));
                if (batch.size() == 2000) { users.insertMany(batch); batch.clear(); }
            }
            if (!batch.isEmpty()) users.insertMany(batch);
        }

        MigrationSpec spec = new MigrationSpec(
                1, MigrationKind.DATA_TRANSFER, "cadence-test",
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
                        new PerfSpec(2, 500_000L, 500, 4L * 1024 * 1024, 0L, 3),
                        VerifySpec.defaults(),
                        ErrorPolicy.defaults(), false, null, List.of()));

        List<Long> progressNanos = new java.util.concurrent.CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(1);
        JobStatus[] terminal = new JobStatus[1];
        bus.onJob(e -> {
            if (e instanceof JobEvent.Progress) progressNanos.add(System.nanoTime());
            if (e instanceof JobEvent.StatusChanged sc && sc.newStatus().isTerminal()) {
                terminal[0] = sc.newStatus();
                done.countDown();
            }
        });

        JobId jobId = service.start(spec);
        assertTrue(done.await(90, TimeUnit.SECONDS), "job did not finish within 90s");
        assertEquals(JobStatus.COMPLETED, terminal[0]);

        // Cadence assertion: over the job's middle (excludes startup + shutdown edges where
        // the first/last ticks may be uneven), we want ≥ 4 events per wall-clock second.
        assertTrue(progressNanos.size() >= 5,
                "too few Progress events to assess cadence: " + progressNanos.size());
        long first = progressNanos.get(0);
        long last = progressNanos.get(progressNanos.size() - 1);
        double elapsedSec = (last - first) / 1e9;
        double eventsPerSec = (progressNanos.size() - 1) / Math.max(elapsedSec, 0.001);
        assertTrue(eventsPerSec >= 4.0,
                String.format("expected ≥ 4 Progress events/sec, got %.2f (%d events over %.2fs)",
                        eventsPerSec, progressNanos.size(), elapsedSec));

        // OBS-5 RUN-mode invariant: docsCopied == docsProcessed.
        MigrationJobRecord rec = service.get(jobId).orElseThrow();
        assertEquals(rec.docsCopied(), rec.docsProcessed(),
                "RUN mode must keep docsCopied and docsProcessed in lock-step");
        assertEquals(DOC_COUNT, rec.docsCopied());
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
        assertTrue(latch.await(30, TimeUnit.SECONDS), "connect timed out for " + id);
        MongoService svc = manager.service(id);
        assertNotNull(svc, "MongoService for " + id);
    }
}
