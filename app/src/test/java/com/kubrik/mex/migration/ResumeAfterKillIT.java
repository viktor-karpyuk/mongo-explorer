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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@code copy_resumes_after_kill}: start a large copy, cancel mid-flight, and resume.
 *  The final target state must contain every source document exactly once with no data
 *  loss, no duplication, and no dependency on the cancelled-run partial progress.
 *  <p>
 *  Tech spec reference: §17.2 test #4. */
@Testcontainers(disabledWithoutDocker = true)
class ResumeAfterKillIT {

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
    void copy_resumes_after_kill() throws Exception {
        // Seed 5 000 docs so the migration runs long enough to be cancelled mid-flight.
        final int total = 5_000;
        try (MongoClient c = MongoClients.create(SOURCE.getConnectionString())) {
            MongoCollection<Document> coll = c.getDatabase("app").getCollection("events");
            List<Document> batch = new java.util.ArrayList<>();
            for (int i = 0; i < total; i++) {
                batch.add(new Document("_id", i).append("payload", "x".repeat(200)));
                if (batch.size() == 500) { coll.insertMany(batch); batch.clear(); }
            }
            if (!batch.isEmpty()) coll.insertMany(batch);
        }

        MigrationSpec spec = new MigrationSpec(
                1,
                MigrationKind.DATA_TRANSFER,
                "resume-test",
                new SourceSpec(sourceId, "primary"),
                new TargetSpec(targetId, null),
                new ScopeSpec.Collections(
                        List.of(com.kubrik.mex.migration.spec.Namespace.parse("app.events")),
                        new com.kubrik.mex.migration.spec.ScopeFlags(false, false),
                        List.of("**"), List.of(), List.of()),
                null,
                new MigrationSpec.Options(
                        ExecutionMode.RUN,
                        // UPSERT_BY_ID so partial+resume is idempotent even if we re-cover a doc.
                        new MigrationSpec.Conflict(ConflictMode.UPSERT_BY_ID, Map.of()),
                        Map.of(),
                        // Tiny batches + a rate limit: forces the job to be slow enough to
                        // cancel before it finishes, and small enough to produce many checkpoints.
                        new PerfSpec(1, 10_000_000L, 100, 1L * 1024 * 1024, 500L, 3),
                        VerifySpec.defaults(),
                        ErrorPolicy.defaults(), false, null, List.of()));

        // --- run 1: start then cancel mid-flight --------------------------------
        AtomicLong progressDocs = new AtomicLong();
        bus.onJob(e -> {
            if (e instanceof JobEvent.LogLine) return;
            // cheapest progress signal available at this layer: jobs.updateProgress runs
            // every 2 s, so instead we poll the service below.
        });

        JobId jobId = service.start(spec);

        long cancelledAt;
        long waitedMs = 0;
        while (true) {
            var rec = service.get(jobId).orElseThrow();
            cancelledAt = rec.docsCopied();
            if (cancelledAt >= 500) break;                       // far enough along to matter
            assertTrue(waitedMs < 30_000, "job did not progress within 30s");
            Thread.sleep(100);
            waitedMs += 100;
        }
        progressDocs.set(cancelledAt);

        CountDownLatch firstDone = new CountDownLatch(1);
        bus.onJob(e -> {
            if (e instanceof JobEvent.StatusChanged sc && sc.jobId().equals(jobId)
                    && sc.newStatus() == JobStatus.CANCELLED) {
                firstDone.countDown();
            }
        });
        service.cancel(jobId);
        assertTrue(firstDone.await(30, TimeUnit.SECONDS), "cancel didn't take effect in 30s");

        // Target should have partial data, not the full set.
        long targetAfterCancel;
        try (MongoClient c = MongoClients.create(TARGET.getConnectionString())) {
            targetAfterCancel = c.getDatabase("app").getCollection("events").countDocuments();
        }
        assertTrue(targetAfterCancel > 0, "expected some docs on target after cancel");
        assertTrue(targetAfterCancel < total, "cancel fired too late — job already completed");

        // --- run 2: resume the same job id --------------------------------------
        CountDownLatch resumeDone = new CountDownLatch(1);
        JobStatus[] terminal = new JobStatus[1];
        bus.onJob(e -> {
            if (e instanceof JobEvent.StatusChanged sc && sc.jobId().equals(jobId)
                    && sc.newStatus().isTerminal() && sc.newStatus() != JobStatus.CANCELLED) {
                terminal[0] = sc.newStatus();
                resumeDone.countDown();
            }
        });
        JobId resumed = service.resume(jobId);
        assertEquals(jobId, resumed, "resume returns the original job id");
        assertTrue(resumeDone.await(90, TimeUnit.SECONDS), "resume did not finish in 90s");
        assertEquals(JobStatus.COMPLETED, terminal[0]);

        // --- assertions ---------------------------------------------------------
        try (MongoClient c = MongoClients.create(TARGET.getConnectionString())) {
            MongoCollection<Document> coll = c.getDatabase("app").getCollection("events");
            assertEquals(total, coll.countDocuments(), "every source doc must be on target exactly once");
            // sample a handful of ids to confirm payload integrity
            for (int probe : new int[] { 0, 1234, total - 1 }) {
                Document d = coll.find(new Document("_id", probe)).first();
                assertNotNull(d, "missing _id=" + probe);
                assertTrue(((String) d.get("payload")).startsWith("x"));
            }
        }
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
        MongoService svc = manager.service(id);
        assertNotNull(svc, "MongoService for " + id);
    }
}
