package com.kubrik.mex.migration;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.migration.events.JobEvent;
import com.kubrik.mex.migration.events.JobStatus;
import com.kubrik.mex.migration.gate.PreconditionGate;
import com.kubrik.mex.migration.spec.ErrorPolicy;
import com.kubrik.mex.migration.spec.ExecutionMode;
import com.kubrik.mex.migration.spec.MigrationKind;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.migration.spec.PerfSpec;
import com.kubrik.mex.migration.spec.SourceSpec;
import com.kubrik.mex.migration.spec.TargetSpec;
import com.kubrik.mex.migration.spec.VerifySpec;
import com.kubrik.mex.migration.versioned.Rollback;
import com.kubrik.mex.migration.versioned.ScriptRepo;
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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Covers the three tech-spec tests for versioned migrations (§17.2 #9/#10/#11). */
@Testcontainers(disabledWithoutDocker = true)
class VersionedMigrationIT {

    @Container
    static MongoDBContainer SERVER = new MongoDBContainer("mongo:latest");

    @TempDir Path dataDir;
    @TempDir Path scriptsDir;

    private Database db;
    private ConnectionStore store;
    private ConnectionManager manager;
    private EventBus bus;
    private MigrationService service;
    private String connId;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        store = new ConnectionStore(db);
        bus = new EventBus();
        Crypto crypto = new Crypto();
        manager = new ConnectionManager(store, bus, crypto);

        connId = "c-" + System.nanoTime();
        store.upsert(uriConn(connId, "local", SERVER.getConnectionString()));
        awaitConnected(connId);

        service = new MigrationService(manager, store, db, bus, new PreconditionGate(store, manager));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (manager != null) manager.closeAll();
        if (db != null) db.close();
    }

    @Test
    void versioned_applies_new_scripts_only() throws Exception {
        writeScript("V1.0.0__init.json", """
                { "ops": [
                    { "op": "createCollection", "collection": "users" },
                    { "op": "createIndex", "collection": "users",
                      "keys": { "email": 1 },
                      "options": { "unique": true, "name": "users_email_u" } }
                ]}
                """);
        writeScript("V1.0.1__seed.json", """
                { "ops": [
                    { "op": "updateMany", "collection": "users",
                      "filter": {},
                      "update": { "$set": { "status": "active" } } }
                ]}
                """);

        runJobAndWait(buildSpec());

        try (MongoClient c = MongoClients.create(SERVER.getConnectionString())) {
            MongoCollection<Document> audit = c.getDatabase("app_db").getCollection("_mongo_explorer_migrations");
            assertEquals(2, audit.countDocuments());
            long success = audit.countDocuments(new Document("status", "SUCCESS"));
            assertEquals(2, success);

            // Second run should apply nothing new.
            runJobAndWait(buildSpec());
            assertEquals(2, audit.countDocuments());
        }
    }

    @Test
    void versioned_rollback_marks_rolled_back() throws Exception {
        writeScript("V1.0.0__init.json", """
                { "ops": [ { "op": "createCollection", "collection": "users" } ]}
                """);
        writeScript("V1.0.1__index.json", """
                { "ops": [ { "op": "createIndex", "collection": "users",
                             "keys": { "name": 1 },
                             "options": { "name": "name_idx" } } ]}
                """);
        writeScript("U1.0.1__index.json", """
                { "ops": [ { "op": "dropIndex", "collection": "users", "name": "name_idx" } ]}
                """);

        runJobAndWait(buildSpec());

        // Rollback to 1.0.0 — should drop the index and mark V1.0.1 as ROLLED_BACK.
        Rollback.Result r = service.rollbackVersioned(connId, "app_db",
                scriptsDir.toString(), "1.0.0");
        assertEquals(1, r.rolledBackVersions().size());
        assertEquals("1.0.1", r.rolledBackVersions().get(0));

        try (MongoClient c = MongoClients.create(SERVER.getConnectionString())) {
            MongoCollection<Document> audit = c.getDatabase("app_db").getCollection("_mongo_explorer_migrations");
            Document row = audit.find(new Document("_id", "1.0.1")).first();
            assertNotNull(row);
            assertEquals("ROLLED_BACK", row.getString("status"));
            assertNotNull(row.get("rolledBackAt"), "rolledBackAt should be stamped");

            // Audit row for V1.0.0 unchanged.
            Document init = audit.find(new Document("_id", "1.0.0")).first();
            assertNotNull(init);
            assertEquals("SUCCESS", init.getString("status"));

            // Index actually dropped?
            boolean idxPresent = false;
            for (Document idx : c.getDatabase("app_db").getCollection("users").listIndexes()) {
                if ("name_idx".equals(idx.getString("name"))) idxPresent = true;
            }
            assertFalse(idxPresent, "name_idx should have been dropped by the U script");
        }
    }

    @Test
    void versioned_rejects_js_scripts() throws Exception {
        writeScript("V1.0.0__legacy.js", "db.users.createIndex({email: 1});");

        ScriptRepo.ScanResult scan = new ScriptRepo().scan(scriptsDir);
        assertTrue(scan.warnings().stream().anyMatch(w -> w.contains("E-19")),
                "scan should warn about .js rejection referencing E-19");
        assertTrue(scan.scripts().isEmpty(), "no V scripts should have been accepted");
    }

    @Test
    void versioned_drift_blocks_further_applies() throws Exception {
        dropTargetDatabase();
        writeScript("V1.0.0__init.json", """
                { "ops": [ { "op": "createCollection", "collection": "users" } ]}
                """);
        runJobAndWait(buildSpec());

        // Tamper with V1.0.0 on disk — checksum now differs from the stored one. Also add a
        // pending V1.0.1 to force the drift gate (drift alone on nothing-to-do is a warning).
        writeScript("V1.0.0__init.json", """
                { "ops": [ { "op": "createCollection", "collection": "users_v2" } ]}
                """);
        writeScript("V1.0.1__index.json", """
                { "ops": [ { "op": "createIndex", "collection": "users",
                             "keys": { "email": 1 },
                             "options": { "name": "email_idx" } } ]}
                """);

        // VER-4 — preflight now surfaces drift before the job starts, so service.start rejects
        // the spec instead of kicking off a run that would fail deep in the engine.
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> service.start(buildSpec()),
                "drift + pending scripts without ignoreDrift should be blocked by preflight");
        assertTrue(ex.getMessage().contains("Checksum drift on V1.0.0"),
                "preflight error should name the drifted script: " + ex.getMessage());
    }

    @Test
    void versioned_env_filter_skips_non_matching_scripts() throws Exception {
        dropTargetDatabase();
        writeScript("V1.0.0__init.json", """
                { "ops": [ { "op": "createCollection", "collection": "users" } ]}
                """);
        writeScript("V1.0.1__prod_only.json", """
                { "env": "prod",
                  "ops": [ { "op": "createIndex", "collection": "users",
                             "keys": { "email": 1 },
                             "options": { "name": "email_idx" } } ]}
                """);
        writeScript("V1.0.2__not_prod.json", """
                { "env": "!prod",
                  "ops": [ { "op": "createCollection", "collection": "dev_fixtures" } ]}
                """);

        // dev env: V1.0.0 (untagged) and V1.0.2 (!prod) should run; V1.0.1 (prod) must be skipped.
        runJobAndWait(buildSpecForEnv("dev"));
        try (MongoClient c = MongoClients.create(SERVER.getConnectionString())) {
            MongoCollection<Document> audit = c.getDatabase("app_db").getCollection("_mongo_explorer_migrations");
            assertEquals(2, audit.countDocuments(new Document("status", "SUCCESS")),
                    "V1.0.0 + V1.0.2 should be applied in dev; V1.0.1 stays unapplied");
            assertNull(audit.find(new Document("_id", "1.0.1")).first(),
                    "prod-gated script must not have an audit row in dev");

            boolean hasDevFixtures = false;
            for (String n : c.getDatabase("app_db").listCollectionNames()) {
                if ("dev_fixtures".equals(n)) hasDevFixtures = true;
            }
            assertTrue(hasDevFixtures, "!prod script should have created dev_fixtures in dev");
        }
    }

    @Test
    void versioned_drift_proceeds_when_ignore_flag_set() throws Exception {
        dropTargetDatabase();
        writeScript("V1.0.0__init.json", """
                { "ops": [ { "op": "createCollection", "collection": "users" } ]}
                """);
        runJobAndWait(buildSpec());

        writeScript("V1.0.0__init.json", """
                { "ops": [ { "op": "createCollection", "collection": "users_v2" } ]}
                """);
        writeScript("V1.0.1__index.json", """
                { "ops": [ { "op": "createIndex", "collection": "users",
                             "keys": { "email": 1 },
                             "options": { "name": "email_idx" } } ]}
                """);

        assertEquals(JobStatus.COMPLETED, runAndCollectTerminal(buildSpec(true)),
                "ignoreDrift=true should let the job apply the new pending script");

        try (MongoClient c = MongoClients.create(SERVER.getConnectionString())) {
            MongoCollection<Document> audit = c.getDatabase("app_db").getCollection("_mongo_explorer_migrations");
            long count = audit.countDocuments(new Document("status", "SUCCESS"));
            assertEquals(2, count, "both V1.0.0 and V1.0.1 should now be SUCCESS");
        }
    }

    // --- helpers -----------------------------------------------------------------

    private MigrationSpec buildSpec() { return buildSpec(false, null); }

    private MigrationSpec buildSpec(boolean ignoreDrift) { return buildSpec(ignoreDrift, null); }

    private MigrationSpec buildSpecForEnv(String environment) { return buildSpec(false, environment); }

    private MigrationSpec buildSpec(boolean ignoreDrift, String environment) {
        return new MigrationSpec(
                1,
                MigrationKind.VERSIONED,
                "versioned-test",
                new SourceSpec(connId, "primary"),
                new TargetSpec(connId, "app_db"),
                null,
                scriptsDir.toString(),
                new MigrationSpec.Options(
                        ExecutionMode.RUN,
                        new MigrationSpec.Conflict(com.kubrik.mex.migration.spec.ConflictMode.ABORT, Map.of()),
                        Map.of(),
                        PerfSpec.defaults(),
                        VerifySpec.defaults(),
                        ErrorPolicy.defaults(), ignoreDrift, environment, java.util.List.of()));
    }

    private void runJobAndWait(MigrationSpec spec) throws Exception {
        assertEquals(JobStatus.COMPLETED, runAndCollectTerminal(spec),
                "last run terminal status");
    }

    private JobStatus runAndCollectTerminal(MigrationSpec spec) throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        JobStatus[] terminal = new JobStatus[1];
        bus.onJob(e -> {
            if (e instanceof JobEvent.StatusChanged sc && sc.newStatus().isTerminal()) {
                terminal[0] = sc.newStatus();
                done.countDown();
            }
        });
        JobId id = service.start(spec);
        assertTrue(done.await(30, TimeUnit.SECONDS), "versioned job did not finish in 30s");
        assertNotNull(id);
        return terminal[0];
    }

    private void dropTargetDatabase() throws Exception {
        // The @Container is static, so a prior test's audit rows linger in `app_db`. Reset the
        // database so each drift test starts from a clean audit slate.
        try (MongoClient c = MongoClients.create(SERVER.getConnectionString())) {
            c.getDatabase("app_db").drop();
        }
    }

    private void writeScript(String name, String body) throws Exception {
        Files.writeString(scriptsDir.resolve(name), body);
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
