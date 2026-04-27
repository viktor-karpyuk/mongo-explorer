package com.kubrik.mex.cli;

import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.migration.profile.ProfileCodec;
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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** End-to-end CLI IT (UX-5). Exercises:
 *  <ul>
 *    <li>Mongo → Mongo copy invoked via {@link MigrateCli#execute()}.</li>
 *    <li>Exit code 0 on {@code COMPLETED}.</li>
 *    <li>JSON lines on stdout include the expected event-type sequence.</li>
 *    <li>{@code --dry-run} completes without writing to the target.</li>
 *    <li>Missing profile file exits 65 (not 64 — that's arg-parsing only).</li>
 *  </ul> */
@Testcontainers(disabledWithoutDocker = true)
class MigrateCliIT {

    @Container
    static MongoDBContainer SOURCE = new MongoDBContainer("mongo:latest");

    @Container
    static MongoDBContainer TARGET = new MongoDBContainer("mongo:latest");

    @TempDir Path dataDir;
    @TempDir Path profileDir;

    private String sourceId;
    private String targetId;

    @BeforeEach
    void setUp() throws Exception {
        // Isolate the CLI from the developer's real ~/.mongo-explorer. AppPaths resolves
        // against user.home so this also contains the plugins + jobs directories.
        System.setProperty("user.home", dataDir.toString());

        sourceId = "src-" + System.nanoTime();
        targetId = "tgt-" + System.nanoTime();

        // Pre-seed the connection store so the CLI-created MigrationService can find them.
        // Open → write → close in its own handle so the CLI's later Database() call doesn't
        // contend on the SQLite file.
        try (Database db = new Database()) {
            ConnectionStore store = new ConnectionStore(db);
            store.upsert(uriConn(sourceId, "src", SOURCE.getConnectionString()));
            store.upsert(uriConn(targetId, "tgt", TARGET.getConnectionString()));
        }
    }

    @AfterEach
    void tearDown() {
        System.clearProperty("user.home");
        // Static @Container shares SOURCE + TARGET across every test
        // in this class. Drop the test data so a happy-path run that
        // copied to TARGET doesn't leak into the next test's
        // "did dry-run skip writes?" assertion.
        try (MongoClient c = MongoClients.create(SOURCE.getConnectionString())) {
            c.getDatabase("app").drop();
        } catch (Exception ignored) {}
        try (MongoClient c = MongoClients.create(TARGET.getConnectionString())) {
            c.getDatabase("app").drop();
        } catch (Exception ignored) {}
    }

    @Test
    void cli_runs_a_real_copy_and_exits_zero() throws Exception {
        int total = 25;
        seed(total);

        Path profile = writeProfile("run-spec", /*dryRun=*/ false, /*sink=*/ null);
        Result result = runCli("--profile", profile.toString());

        assertEquals(0, result.exit, "happy-path exit: " + result.err);
        assertTrue(result.out.contains("\"type\":\"Started\""),
                "stream must include Started event, got:\n" + result.out);
        assertTrue(result.out.contains("\"type\":\"StatusChanged\""),
                "stream must include StatusChanged events");
        assertTrue(result.out.contains("\"newStatus\":\"RUNNING\""),
                "stream must report RUNNING at some point");
        assertTrue(result.out.contains("\"newStatus\":\"COMPLETED\""),
                "stream must report COMPLETED");

        try (MongoClient c = MongoClients.create(TARGET.getConnectionString())) {
            assertEquals(total, c.getDatabase("app").getCollection("users").countDocuments(),
                    "docs should be copied to the target");
        }
    }

    @Test
    void cli_dry_run_skips_target_writes() throws Exception {
        seed(10);
        Path profile = writeProfile("dry-run-spec", /*dryRun=*/ false, /*sink=*/ null);

        Result result = runCli("--profile", profile.toString(), "--dry-run");

        assertEquals(0, result.exit, "dry-run exits zero: " + result.err);
        try (MongoClient c = MongoClients.create(TARGET.getConnectionString())) {
            boolean hasUsers = false;
            for (String n : c.getDatabase("app").listCollectionNames()) {
                if ("users".equals(n)) hasUsers = true;
            }
            assertFalse(hasUsers, "--dry-run must not write to the target");
        }
    }

    @Test
    void cli_ndjson_sink_writes_file_and_exits_zero() throws Exception {
        int total = 15;
        seed(total);

        Path sinkDir = profileDir.resolve("export");
        Path profile = writeProfile("sink-spec", /*dryRun=*/ false,
                new SinkSpec(SinkSpec.SinkKind.NDJSON, sinkDir.toString()));

        Result result = runCli("--profile", profile.toString());
        assertEquals(0, result.exit, "sink run exits zero: " + result.err);

        Path out = sinkDir.resolve("app.users.ndjson");
        assertTrue(Files.exists(out), "sink file should exist at " + out);
        List<String> lines = Files.readAllLines(out);
        assertEquals(total, lines.size(), "one line per source doc");
    }

    @Test
    void cli_missing_profile_file_exits_65() throws Exception {
        Path absent = profileDir.resolve("does-not-exist.yaml");
        Result result = runCli("--profile", absent.toString());
        assertEquals(65, result.exit);
        assertTrue(result.err.contains("profile file not found"),
                "err should name the missing file: " + result.err);
    }

    // --- helpers -----------------------------------------------------------------

    private void seed(int total) {
        try (MongoClient c = MongoClients.create(SOURCE.getConnectionString())) {
            MongoCollection<Document> users = c.getDatabase("app").getCollection("users");
            users.drop();
            for (int i = 0; i < total; i++) {
                users.insertOne(new Document("_id", i).append("name", "user-" + i));
            }
        }
    }

    private Path writeProfile(String name, boolean dryRun, SinkSpec sink) throws Exception {
        MigrationSpec spec = new MigrationSpec(
                1, MigrationKind.DATA_TRANSFER, name,
                new SourceSpec(sourceId, "primary"),
                new TargetSpec(targetId, null),
                new ScopeSpec.Collections(
                        List.of(Namespace.parse("app.users")),
                        new ScopeFlags(false, false),
                        List.of("**"), List.of(), List.of()),
                null,
                new MigrationSpec.Options(
                        dryRun ? ExecutionMode.DRY_RUN : ExecutionMode.RUN,
                        new MigrationSpec.Conflict(ConflictMode.APPEND, Map.of()),
                        Map.of(),
                        PerfSpec.defaults(),
                        VerifySpec.defaults(),
                        ErrorPolicy.defaults(), false, null,
                        sink == null ? List.of() : List.of(sink)));
        String yaml = new ProfileCodec().toYaml(spec);
        Path file = profileDir.resolve(name + ".yaml");
        Files.writeString(file, yaml);
        return file;
    }

    private Result runCli(String... args) {
        MigrateCli cli = new MigrateCli();
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        cli.out = new PrintWriter(out);
        cli.err = new PrintWriter(err);
        new picocli.CommandLine(cli).parseArgs(args);
        int exit = cli.execute();
        return new Result(exit, out.toString(), err.toString());
    }

    private record Result(int exit, String out, String err) {}

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
}
