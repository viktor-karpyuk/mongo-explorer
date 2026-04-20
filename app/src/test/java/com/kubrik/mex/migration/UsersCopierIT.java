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

import static org.junit.jupiter.api.Assertions.*;

/** P4.6 — SCOPE-12 UsersCopier round-trip. Verifies that a non-built-in user on the source DB
 *  lands on the corresponding target DB when {@code migrateUsers = true}, that per-user
 *  failures (duplicate existing user) degrade the job to {@code COMPLETED_WITH_WARNINGS}
 *  without killing it, and that the {@code __system} built-in is skipped. */
@Testcontainers(disabledWithoutDocker = true)
class UsersCopierIT {

    @Container static MongoDBContainer SOURCE = new MongoDBContainer("mongo:7.0");
    @Container static MongoDBContainer TARGET = new MongoDBContainer("mongo:7.0");

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
    void copies_non_builtin_user_and_skips_system() throws Exception {
        String appDb = "app_copy_" + System.nanoTime();
        seedOneDoc(SOURCE.getConnectionString(), appDb, "widgets");
        createUser(SOURCE.getConnectionString(), appDb, "alice", "secret1",
                List.of(new Document("role", "readWrite").append("db", appDb)));

        MigrationSpec spec = specWithUsers(appDb);
        JobStatus terminal = runUntilTerminal(spec);

        assertEquals(JobStatus.COMPLETED, terminal);
        assertTrue(userExists(TARGET.getConnectionString(), appDb, "alice"),
                "alice must be copied to the target's matching DB");
        assertFalse(userExists(TARGET.getConnectionString(), appDb, "__system"),
                "built-in __system must not be copied");

        var rec = service.get(JobId.of(lastJobId)).orElseThrow();
        assertEquals(0, rec.errors());
    }

    @Test
    void duplicate_user_on_target_degrades_to_warnings() throws Exception {
        String appDb = "app_dup_" + System.nanoTime();
        seedOneDoc(SOURCE.getConnectionString(), appDb, "widgets");
        createUser(SOURCE.getConnectionString(), appDb, "bob", "pw",
                List.of(new Document("role", "readWrite").append("db", appDb)));
        // Pre-create the same user on target → createUser will fail with "already exists".
        createUser(TARGET.getConnectionString(), appDb, "bob", "different",
                List.of(new Document("role", "read").append("db", appDb)));

        MigrationSpec spec = specWithUsers(appDb);
        JobStatus terminal = runUntilTerminal(spec);

        assertEquals(JobStatus.COMPLETED_WITH_WARNINGS, terminal,
                "a per-user failure must degrade the job, not kill it");
    }

    private MigrationSpec specWithUsers(String sourceDb) {
        return new MigrationSpec(
                1, MigrationKind.DATA_TRANSFER, "users-copy-test",
                new SourceSpec(sourceId, "primary"),
                new TargetSpec(targetId, null),
                new ScopeSpec.Collections(
                        List.of(new Namespace(sourceDb, "widgets")),
                        new ScopeFlags(true, true),
                        List.of("**"), List.of(), List.of()),
                null,
                new MigrationSpec.Options(
                        ExecutionMode.RUN,
                        new MigrationSpec.Conflict(ConflictMode.APPEND, Map.of()),
                        Map.of(),
                        PerfSpec.defaults(),
                        new VerifySpec(false, 0, false),
                        ErrorPolicy.defaults(), false, null, List.of()));
    }

    private String lastJobId;

    private JobStatus runUntilTerminal(MigrationSpec spec) throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        JobStatus[] terminal = new JobStatus[1];
        bus.onJob(e -> {
            if (e instanceof JobEvent.StatusChanged sc && sc.newStatus().isTerminal()) {
                terminal[0] = sc.newStatus();
                done.countDown();
            }
        });
        JobId id = service.start(spec);
        lastJobId = id.value();
        assertTrue(done.await(60, TimeUnit.SECONDS), "job did not finish within 60s");
        return terminal[0];
    }

    private void seedOneDoc(String uri, String db, String coll) {
        try (MongoClient c = MongoClients.create(uri)) {
            MongoCollection<Document> col = c.getDatabase(db).getCollection(coll);
            col.insertOne(new Document("_id", 1).append("name", "w1"));
        }
    }

    private void createUser(String uri, String db, String user, String pwd, List<Document> roles) {
        try (MongoClient c = MongoClients.create(uri)) {
            c.getDatabase(db).runCommand(new Document("createUser", user)
                    .append("pwd", pwd)
                    .append("roles", roles));
        }
    }

    @SuppressWarnings("unchecked")
    private boolean userExists(String uri, String db, String user) {
        try (MongoClient c = MongoClients.create(uri)) {
            Document resp = c.getDatabase(db).runCommand(new Document("usersInfo", user));
            Object raw = resp.get("users");
            return raw instanceof List && !((List<Document>) raw).isEmpty();
        } catch (Exception e) {
            return false;
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
        assertNotNull(svc, "MongoService for " + id);
    }
}
