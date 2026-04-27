package com.kubrik.mex.migration;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.Crypto;
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
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;
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

/** SCOPE-5 — end-to-end coverage for collection-option propagation. Verifies that capped,
 *  validator + validationLevel + validationAction, and collation flow from source to target
 *  when the target is freshly created (DROP_AND_RECREATE or first write to a new namespace). */
@Testcontainers(disabledWithoutDocker = true)
class CollectionOptionsIT {

    @Container
    static MongoDBContainer SOURCE = new MongoDBContainer("mongo:latest");

    @Container
    static MongoDBContainer TARGET = new MongoDBContainer("mongo:latest");

    @TempDir Path dataDir;

    private Database db;
    private ConnectionManager manager;
    private EventBus bus;
    private MigrationService service;
    private String sourceId;
    private String targetId;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        ConnectionStore store = new ConnectionStore(db);
        bus = new EventBus();
        manager = new ConnectionManager(store, bus, new Crypto());

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
        System.clearProperty("user.home");
    }

    @Test
    void capped_collection_carries_options_to_target() throws Exception {
        // Source: capped with 1 MB cap, max 500 docs.
        try (MongoClient c = MongoClients.create(SOURCE.getConnectionString())) {
            MongoDatabase sdb = c.getDatabase("app");
            sdb.createCollection("events", new CreateCollectionOptions()
                    .capped(true).sizeInBytes(1024 * 1024).maxDocuments(500));
            for (int i = 0; i < 25; i++) {
                sdb.getCollection("events").insertOne(new Document("_id", i).append("name", "evt-" + i));
            }
        }

        runDataCopy("app.events");

        try (MongoClient c = MongoClients.create(TARGET.getConnectionString())) {
            Document opts = collectionOptions(c.getDatabase("app"), "events");
            assertTrue(opts.getBoolean("capped", false), "target should be capped: " + opts);
            assertEquals(1024L * 1024L, ((Number) opts.get("size")).longValue());
            assertEquals(500L, ((Number) opts.get("max")).longValue());
        }
    }

    @Test
    void validator_level_and_action_propagate() throws Exception {
        Document validator = Document.parse(
                "{ $jsonSchema: { bsonType: 'object', required: ['name'], "
              + "properties: { name: { bsonType: 'string' } } } }");
        try (MongoClient c = MongoClients.create(SOURCE.getConnectionString())) {
            MongoDatabase sdb = c.getDatabase("app");
            sdb.createCollection("users", new CreateCollectionOptions()
                    .validationOptions(new ValidationOptions()
                            .validator(validator)
                            .validationLevel(com.mongodb.client.model.ValidationLevel.MODERATE)
                            .validationAction(com.mongodb.client.model.ValidationAction.WARN)));
            sdb.getCollection("users").insertOne(new Document("_id", 1).append("name", "alice"));
        }

        runDataCopy("app.users");

        try (MongoClient c = MongoClients.create(TARGET.getConnectionString())) {
            Document opts = collectionOptions(c.getDatabase("app"), "users");
            assertNotNull(opts.get("validator"), "validator should be copied");
            assertEquals("moderate", opts.getString("validationLevel"));
            assertEquals("warn", opts.getString("validationAction"));
        }
    }

    @Test
    void collation_propagates() throws Exception {
        Document collation = new Document("locale", "en").append("strength", 2);
        try (MongoClient c = MongoClients.create(SOURCE.getConnectionString())) {
            MongoDatabase sdb = c.getDatabase("app");
            sdb.createCollection("words", new CreateCollectionOptions()
                    .collation(com.mongodb.client.model.Collation.builder()
                            .locale("en")
                            .collationStrength(com.mongodb.client.model.CollationStrength.SECONDARY)
                            .build()));
            sdb.getCollection("words").insertOne(new Document("_id", 1).append("w", "hello"));
        }

        runDataCopy("app.words");

        try (MongoClient c = MongoClients.create(TARGET.getConnectionString())) {
            Document opts = collectionOptions(c.getDatabase("app"), "words");
            Document tgtCollation = (Document) opts.get("collation");
            assertNotNull(tgtCollation, "collation should be copied: " + opts);
            assertEquals("en", tgtCollation.getString("locale"));
            assertEquals(2, ((Number) tgtCollation.get("strength")).intValue());
        }
    }

    @Test
    void plain_collection_needs_no_options() throws Exception {
        try (MongoClient c = MongoClients.create(SOURCE.getConnectionString())) {
            MongoDatabase sdb = c.getDatabase("app");
            sdb.createCollection("plain");
            for (int i = 0; i < 5; i++) {
                sdb.getCollection("plain").insertOne(new Document("_id", i));
            }
        }

        runDataCopy("app.plain");

        try (MongoClient c = MongoClients.create(TARGET.getConnectionString())) {
            Document opts = collectionOptions(c.getDatabase("app"), "plain");
            assertFalse(opts.getBoolean("capped", false), "plain collection should not be capped");
            assertNull(opts.get("validator"));
            assertEquals(5, c.getDatabase("app").getCollection("plain").countDocuments(),
                    "docs should still have been copied");
        }
    }

    // --- helpers -----------------------------------------------------------------

    private Document collectionOptions(MongoDatabase db, String coll) {
        for (Document d : db.listCollections().filter(new Document("name", coll))) {
            Object opts = d.get("options");
            return opts instanceof Document doc ? doc : new Document();
        }
        return new Document();
    }

    private void runDataCopy(String namespace) throws Exception {
        MigrationSpec spec = new MigrationSpec(
                1, MigrationKind.DATA_TRANSFER, "opts-test",
                new SourceSpec(sourceId, "primary"),
                new TargetSpec(targetId, null),
                new ScopeSpec.Collections(
                        List.of(Namespace.parse(namespace)),
                        new ScopeFlags(false, false),
                        List.of("**"), List.of(), List.of()),
                null,
                new MigrationSpec.Options(
                        ExecutionMode.RUN,
                        new MigrationSpec.Conflict(ConflictMode.APPEND, Map.of()),
                        Map.of(),
                        PerfSpec.defaults(),
                        VerifySpec.defaults(),
                        ErrorPolicy.defaults(), false, null, List.of()));

        CountDownLatch done = new CountDownLatch(1);
        JobStatus[] terminal = new JobStatus[1];
        bus.onJob(e -> {
            if (e instanceof JobEvent.StatusChanged sc && sc.newStatus().isTerminal()) {
                terminal[0] = sc.newStatus();
                done.countDown();
            }
        });
        service.start(spec);
        assertTrue(done.await(60, TimeUnit.SECONDS), "migration did not finish within 60s");
        assertEquals(JobStatus.COMPLETED, terminal[0],
                "migration should complete for " + namespace);
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
