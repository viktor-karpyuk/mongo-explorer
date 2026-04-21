package com.kubrik.mex.backup;

import com.kubrik.mex.backup.store.SinkDao;
import com.kubrik.mex.backup.store.SinkRecord;
import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.5 STG-1..5 — SinkDao round-trip + credential encryption.
 *
 * <p>Covers {@code insert} / {@code byId} / {@code byName} / {@code listAll} /
 * {@code update} / {@code delete}; verifies that {@code credentials_enc} on
 * disk is ciphertext (not the plain JSON) so a misconfigured build can't
 * ship tokens in the clear.</p>
 */
class SinkDaoTest {

    @TempDir Path dataDir;

    private Database db;
    private Crypto crypto;
    private SinkDao dao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        crypto = new Crypto();
        dao = new SinkDao(db, crypto);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void insert_then_byId_and_byName_round_trip() {
        SinkRecord in = new SinkRecord(-1, "S3", "prod-nightly",
                "s3://example-backups/",
                "{\"accessKeyId\":\"AKIA\",\"secretAccessKey\":\"shhh\"}",
                "{\"region\":\"us-east-1\"}",
                1_000L, 1_000L);

        SinkRecord saved = dao.insert(in);
        assertTrue(saved.id() > 0);

        Optional<SinkRecord> byId = dao.byId(saved.id());
        assertTrue(byId.isPresent());
        assertEquals("prod-nightly", byId.get().name());
        assertEquals(in.credentialsJson(), byId.get().credentialsJson(),
                "credentials decrypt back to plain text");
        assertEquals("{\"region\":\"us-east-1\"}", byId.get().extrasJson());

        Optional<SinkRecord> byName = dao.byName("prod-nightly");
        assertTrue(byName.isPresent());
        assertEquals(saved.id(), byName.get().id());
    }

    @Test
    void credentials_on_disk_are_not_plain_text() throws Exception {
        SinkRecord in = new SinkRecord(-1, "S3", "secret-sink", "s3://x/",
                "super-secret-token", null, 1L, 1L);
        dao.insert(in);
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT credentials_enc FROM storage_sinks WHERE name = ?")) {
            ps.setString(1, "secret-sink");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                byte[] blob = rs.getBytes(1);
                assertNotNull(blob);
                String onDisk = new String(blob, java.nio.charset.StandardCharsets.UTF_8);
                assertFalse(onDisk.contains("super-secret-token"),
                        "plain-text token must not appear in the stored blob");
            }
        }
    }

    @Test
    void local_sink_has_null_credentials() {
        SinkRecord in = new SinkRecord(-1, "LOCAL", "local-nightly",
                "/var/backups/mex", null, null, 1L, 1L);
        SinkRecord saved = dao.insert(in);
        assertNull(dao.byId(saved.id()).orElseThrow().credentialsJson());
    }

    @Test
    void update_replaces_credentials_and_extras() {
        SinkRecord first = dao.insert(new SinkRecord(-1, "S3", "rotating",
                "s3://x/", "v1-token", "{\"r\":\"us-east-1\"}", 1L, 1L));
        SinkRecord rotated = new SinkRecord(first.id(), "S3", "rotating",
                "s3://x/", "v2-token", "{\"r\":\"us-west-2\"}", 1L, 2L);
        dao.update(rotated);

        SinkRecord back = dao.byId(first.id()).orElseThrow();
        assertEquals("v2-token", back.credentialsJson());
        assertEquals("{\"r\":\"us-west-2\"}", back.extrasJson());
        assertEquals(2L, back.updatedAt());
    }

    @Test
    void delete_is_idempotent_and_removes_the_row() {
        SinkRecord saved = dao.insert(new SinkRecord(-1, "LOCAL", "gone",
                "/tmp", null, null, 1L, 1L));
        assertTrue(dao.delete(saved.id()));
        assertTrue(dao.byId(saved.id()).isEmpty());
        assertFalse(dao.delete(saved.id()), "second delete is a no-op");
    }

    @Test
    void listAll_returns_sinks_sorted_by_name() {
        dao.insert(new SinkRecord(-1, "LOCAL", "z-sink", "/a", null, null, 1L, 1L));
        dao.insert(new SinkRecord(-1, "LOCAL", "a-sink", "/b", null, null, 1L, 1L));
        var list = dao.listAll();
        assertEquals(2, list.size());
        assertEquals("a-sink", list.get(0).name());
        assertEquals("z-sink", list.get(1).name());
    }
}
