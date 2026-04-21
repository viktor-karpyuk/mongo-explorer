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
import org.bson.RawBsonDocument;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** EXT-2 — end-to-end coverage of the non-NDJSON sink formats (JSON array, CSV, BSON dump).
 *  The NDJSON path is covered by {@code NdjsonSinkIT}. Each test seeds a small collection,
 *  runs a sink-only job, and verifies the on-disk format-specific invariants. */
@Testcontainers(disabledWithoutDocker = true)
class FileSinkFormatsIT {

    @Container
    static MongoDBContainer SOURCE = new MongoDBContainer("mongo:7.0");

    @Container
    static MongoDBContainer TARGET = new MongoDBContainer("mongo:7.0");

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
    }

    @Test
    void exports_as_json_array() throws Exception {
        int total = seedUsers(20);
        runSink(SinkSpec.SinkKind.JSON_ARRAY);

        Path out = sinkDir.resolve("app.users.json");
        assertTrue(Files.exists(out), "JSON array file should exist at " + out);

        String text = Files.readString(out);
        assertTrue(text.startsWith("["), "must start with [");
        assertTrue(text.endsWith("]"), "must end with ]");

        // Parsing as a bson Document array — the driver understands JSON extended form.
        // `{"docs": <array>}` wrapping keeps the parser happy since Document.parse wants an
        // object at the root.
        Document wrapped = Document.parse("{\"docs\":" + text + "}");
        @SuppressWarnings("unchecked")
        List<Document> docs = (List<Document>) wrapped.get("docs");
        assertEquals(total, docs.size(), "one entry per source document");
        assertNotNull(docs.get(0).get("_id"));
        assertTrue(docs.get(0).getString("name").startsWith("user-"));
    }

    @Test
    void exports_as_csv_with_header_and_one_row_per_doc() throws Exception {
        int total = seedUsers(10);
        runSink(SinkSpec.SinkKind.CSV);

        Path out = sinkDir.resolve("app.users.csv");
        assertTrue(Files.exists(out), "CSV file should exist at " + out);

        List<String> lines = Files.readAllLines(out);
        assertEquals(total + 1, lines.size(), "header + one row per document");

        String header = lines.get(0);
        assertTrue(header.contains("_id"), "header must include _id: " + header);
        assertTrue(header.contains("name"), "header must include name: " + header);

        // Rough sanity: every data row is non-empty and has the same comma count as the
        // header (no column leaks or breaks).
        int expectedCommas = countChar(header, ',');
        for (int i = 1; i < lines.size(); i++) {
            String row = lines.get(i);
            assertFalse(row.isBlank(), "no blank rows");
            assertEquals(expectedCommas, countChar(stripQuoted(row), ','),
                    "row " + i + " column count: " + row);
        }
    }

    @Test
    void exports_as_bson_dump_readable_as_concatenated_bson() throws Exception {
        int total = seedUsers(15);
        runSink(SinkSpec.SinkKind.BSON_DUMP);

        Path out = sinkDir.resolve("app.users.bson");
        assertTrue(Files.exists(out), "BSON dump should exist at " + out);

        byte[] all = Files.readAllBytes(out);
        assertTrue(all.length > 0, "file should be non-empty");

        // Each BSON doc starts with a 4-byte little-endian length header covering the whole
        // doc — walking length-prefixed is the wire format mongorestore reads.
        int pos = 0, count = 0;
        List<String> names = new ArrayList<>();
        while (pos < all.length) {
            assertTrue(pos + 4 <= all.length, "truncated length header at offset " + pos);
            int len = ByteBuffer.wrap(all, pos, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
            assertTrue(len > 0 && pos + len <= all.length,
                    "invalid doc length " + len + " at offset " + pos);
            byte[] slice = new byte[len];
            System.arraycopy(all, pos, slice, 0, len);
            RawBsonDocument d = new RawBsonDocument(slice);
            // Round-trip sanity — relaxed JSON must parse as a document with expected fields.
            Document parsed = Document.parse(d.toJson(JsonWriterSettings.builder().build()));
            assertNotNull(parsed.get("_id"));
            names.add(parsed.getString("name"));
            count++;
            pos += len;
        }
        assertEquals(total, count, "document count round-trip");
        assertTrue(names.stream().allMatch(n -> n != null && n.startsWith("user-")));
    }

    // --- helpers -----------------------------------------------------------------

    private int seedUsers(int total) {
        try (MongoClient c = MongoClients.create(SOURCE.getConnectionString())) {
            MongoCollection<Document> users = c.getDatabase("app").getCollection("users");
            users.drop();
            for (int i = 0; i < total; i++) {
                users.insertOne(new Document("_id", i)
                        .append("name", "user-" + i)
                        .append("v", i % 5));
            }
        }
        return total;
    }

    private void runSink(SinkSpec.SinkKind kind) throws Exception {
        MigrationSpec spec = new MigrationSpec(
                1,
                MigrationKind.DATA_TRANSFER,
                "sink-" + kind,
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
                        List.of(new SinkSpec(kind, sinkDir.toString()))));

        CountDownLatch done = new CountDownLatch(1);
        JobStatus[] terminal = new JobStatus[1];
        bus.onJob(e -> {
            if (e instanceof JobEvent.StatusChanged sc && sc.newStatus().isTerminal()) {
                terminal[0] = sc.newStatus();
                done.countDown();
            }
        });
        service.start(spec);
        assertTrue(done.await(60, TimeUnit.SECONDS), kind + " sink did not finish within 60s");
        assertEquals(JobStatus.COMPLETED, terminal[0]);
    }

    private static int countChar(String s, char c) {
        int n = 0;
        for (int i = 0; i < s.length(); i++) if (s.charAt(i) == c) n++;
        return n;
    }

    /** Strip everything inside RFC 4180 double-quoted sections so the caller can count the
     *  structural commas that separate columns, ignoring any that appear inside a field value. */
    private static String stripQuoted(String row) {
        StringBuilder sb = new StringBuilder(row.length());
        boolean inQuote = false;
        for (int i = 0; i < row.length(); i++) {
            char c = row.charAt(i);
            if (c == '"') {
                // RFC 4180 escape: "" inside a quoted cell is a literal quote
                if (inQuote && i + 1 < row.length() && row.charAt(i + 1) == '"') { i++; continue; }
                inQuote = !inQuote;
                continue;
            }
            if (!inQuote) sb.append(c);
        }
        return sb.toString();
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
