package com.kubrik.mex.migration;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.migration.events.JobEvent;
import com.kubrik.mex.migration.events.JobStatus;
import com.kubrik.mex.migration.gate.PreconditionGate;
import com.kubrik.mex.migration.log.Redactor;
import com.kubrik.mex.migration.preflight.PreflightReport;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** M-4 verifier + preflight coverage (tech spec §17.2 #12, #13; plus a redactor fuzz). */
@Testcontainers(disabledWithoutDocker = true)
class VerifierIT {

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
    void verify_sample_diff_zero_on_identical_data() throws Exception {
        seedSource(200);
        MigrationSpec spec = dataSpec();
        runAndAwait(spec);

        Path jobDir = firstJobDir();
        assertTrue(Files.exists(jobDir.resolve("verification.json")),
                "verification.json must be written for non-dry runs");
        assertTrue(Files.exists(jobDir.resolve("verification.html")),
                "verification.html must be written for non-dry runs");
        String json = Files.readString(jobDir.resolve("verification.json"));
        assertTrue(json.contains("\"status\" : \"PASS\"")
                        || json.contains("\"status\":\"PASS\""),
                "verifier should report PASS on an identical copy, got: " + json);
    }

    @Test
    void verify_detects_count_mismatch() throws Exception {
        seedSource(100);
        // Pre-populate the target with an extra doc so the post-migration count doesn't match
        // even after we copy everything cleanly.
        try (MongoClient c = MongoClients.create(TARGET.getConnectionString())) {
            c.getDatabase("app").getCollection("widgets")
                    .insertOne(new Document("_id", 99999).append("extra", true));
        }

        MigrationSpec spec = dataSpec(ConflictMode.APPEND); // target non-empty, append OK
        JobStatus status = runAndAwait(spec);

        // 100 source + 1 extra on target → counts will differ. Verifier downgrades to FAILED.
        assertEquals(JobStatus.FAILED, status, "verifier should fail when counts differ");

        Path jobDir = firstJobDir();
        String json = Files.readString(jobDir.resolve("verification.json"));
        assertTrue(json.contains("\"countMatch\" : false")
                        || json.contains("\"countMatch\":false"),
                "verifier should flag countMatch=false, got: " + json);
    }

    @Test
    void verify_full_hash_compare_passes_on_identical_data() throws Exception {
        seedSource(150);
        MigrationSpec spec = dataSpec(ConflictMode.ABORT, new VerifySpec(true, 20, true));
        JobStatus status = runAndAwait(spec);

        assertEquals(JobStatus.COMPLETED, status, "identical copy with full-hash compare should PASS");
        Path jobDir = firstJobDir();
        String json = Files.readString(jobDir.resolve("verification.json"));
        assertTrue(json.contains("\"fullHash\" : \"sha256:")
                        || json.contains("\"fullHash\":\"sha256:"),
                "verifier should emit sha256: fullHash on matching data, got: " + json);
        assertFalse(json.contains("MISMATCH"), "no MISMATCH expected on identical copy");
    }

    @Test
    void preflight_warns_about_partial_filter_indexes() throws Exception {
        seedSource(5);
        // Create a partial-filter index on the source collection.
        try (MongoClient c = MongoClients.create(SOURCE.getConnectionString())) {
            com.mongodb.client.model.IndexOptions opts = new com.mongodb.client.model.IndexOptions()
                    .partialFilterExpression(new Document("active", true));
            c.getDatabase("app").getCollection("widgets")
                    .createIndex(new Document("name", 1), opts);
        }
        PreflightReport report = service.preflight(dataSpec());
        assertTrue(report.warnings().stream().anyMatch(w -> w.contains("partial filter")),
                "preflight should warn about partial filter indexes, got: " + report.warnings());
    }

    @Test
    void preflight_blocks_same_cluster_same_namespace() {
        seedSource(10);
        // source == target connection, same namespace
        MigrationSpec spec = new MigrationSpec(
                1, MigrationKind.DATA_TRANSFER, "self",
                new SourceSpec(sourceId, "primary"),
                new TargetSpec(sourceId, null),
                new ScopeSpec.Collections(
                        List.of(com.kubrik.mex.migration.spec.Namespace.parse("app.widgets")),
                        new com.kubrik.mex.migration.spec.ScopeFlags(false, false),
                        List.of("**"), List.of(), List.of()),
                null,
                new MigrationSpec.Options(
                        ExecutionMode.RUN,
                        new MigrationSpec.Conflict(ConflictMode.ABORT, Map.of()),
                        Map.of(),
                        PerfSpec.defaults(),
                        VerifySpec.defaults(),
                        ErrorPolicy.defaults(), false, null, List.of()));
        PreflightReport report = service.preflight(spec);
        assertFalse(report.errors().isEmpty(),
                "preflight must error on same-cluster + same-namespace");
        assertTrue(report.errors().stream()
                        .anyMatch(e -> e.contains("same namespace on the same cluster")),
                "error should mention the same-cluster collision, got: " + report.errors());
    }

    @Test
    void redactor_scrubs_uri_passwords_and_pii_keys() {
        Redactor r = Redactor.defaultInstance();
        String line = "connected to mongodb://vik:s3cret!@atlas.example.com:27017 "
                + "with {\"password\":\"hunter2\",\"email\":\"x@y.z\"}";
        String out = r.redact(line);
        assertFalse(out.contains("s3cret!"), "URI password must be scrubbed");
        assertFalse(out.contains("hunter2"), "PII-key value must be scrubbed");
        assertTrue(out.contains("atlas.example.com"), "non-secret host stays");
    }

    // --- helpers -----------------------------------------------------------------

    private MigrationSpec dataSpec() { return dataSpec(ConflictMode.ABORT); }

    private MigrationSpec dataSpec(ConflictMode mode) {
        return dataSpec(mode, new VerifySpec(true, 50, false));
    }

    private MigrationSpec dataSpec(ConflictMode mode, VerifySpec verify) {
        return new MigrationSpec(
                1, MigrationKind.DATA_TRANSFER, "verify-test",
                new SourceSpec(sourceId, "primary"),
                new TargetSpec(targetId, null),
                new ScopeSpec.Collections(
                        List.of(com.kubrik.mex.migration.spec.Namespace.parse("app.widgets")),
                        new com.kubrik.mex.migration.spec.ScopeFlags(false, false),
                        List.of("**"), List.of(), List.of()),
                null,
                new MigrationSpec.Options(
                        ExecutionMode.RUN,
                        new MigrationSpec.Conflict(mode, Map.of()),
                        Map.of(),
                        new PerfSpec(1, 10_000_000L, 100, 1L * 1024 * 1024, 0L, 3),
                        verify,
                        ErrorPolicy.defaults(), false, null, List.of()));
    }

    private JobStatus runAndAwait(MigrationSpec spec) throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        JobStatus[] terminal = new JobStatus[1];
        bus.onJob(e -> {
            if (e instanceof JobEvent.StatusChanged sc && sc.newStatus().isTerminal()) {
                terminal[0] = sc.newStatus();
                done.countDown();
            }
        });
        service.start(spec);
        assertTrue(done.await(60, TimeUnit.SECONDS), "job didn't finish in 60s");
        return terminal[0];
    }

    private void seedSource(int count) {
        // Containers are shared across tests via @Container static. Drop before seeding so
        // each test starts clean on its own collection.
        try (MongoClient c = MongoClients.create(SOURCE.getConnectionString())) {
            c.getDatabase("app").getCollection("widgets").drop();
            MongoCollection<Document> coll = c.getDatabase("app").getCollection("widgets");
            for (int i = 0; i < count; i++) {
                coll.insertOne(new Document("_id", i).append("name", "w-" + i));
            }
        }
        try (MongoClient c = MongoClients.create(TARGET.getConnectionString())) {
            c.getDatabase("app").getCollection("widgets").drop();
        }
    }

    private Path firstJobDir() throws Exception {
        Path macPath = dataDir.resolve("Library/Application Support/MongoExplorer/jobs");
        Path linuxPath = dataDir.resolve(".local/share/mongo-explorer/jobs");
        Path jobs = Files.exists(macPath) ? macPath : linuxPath;
        final Path finalJobs = jobs;
        try (var s = Files.list(jobs)) {
            return s.findFirst().orElseThrow(() -> new AssertionError("no job dir found under " + finalJobs));
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
        assertTrue(latch.await(30, TimeUnit.SECONDS), "connect timed out for " + id);
        MongoService svc = manager.service(id);
        assertNotNull(svc);
    }
}
