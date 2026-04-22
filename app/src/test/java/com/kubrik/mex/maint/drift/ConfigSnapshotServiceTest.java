package com.kubrik.mex.maint.drift;

import com.kubrik.mex.maint.model.ConfigSnapshot;
import com.kubrik.mex.store.Database;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConfigSnapshotServiceTest {

    @TempDir Path dataDir;

    private Database db;
    private ConfigSnapshotDao dao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        dao = new ConfigSnapshotDao(db);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    /* =========================== canonicalization =========================== */

    @Test
    void canonicalize_sorts_keys_alphabetically() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("zeta", 1);
        m.put("alpha", 2);
        m.put("middle", 3);
        String json = ConfigSnapshotService.canonicalizeForTest(m);
        // "alpha" comes before "middle" comes before "zeta".
        assertTrue(json.indexOf("alpha") < json.indexOf("middle"));
        assertTrue(json.indexOf("middle") < json.indexOf("zeta"));
    }

    @Test
    void canonicalize_recurses_into_nested_documents() {
        Document inner = new Document("zz", 1).append("aa", 2);
        Document outer = new Document("b", inner).append("a", 9);
        String json = ConfigSnapshotService.canonicalize(outer);
        // Outer keys sorted.
        assertTrue(json.indexOf("\"a\":") < json.indexOf("\"b\":"));
        // Inner keys too — "aa" before "zz" regardless of input order.
        int aa = json.indexOf("\"aa\":");
        int zz = json.indexOf("\"zz\":");
        assertTrue(aa > 0 && zz > aa);
    }

    @Test
    void canonicalize_hash_is_stable_across_runs() {
        Document d = new Document("x", 1).append("y", 2);
        String a = ConfigSnapshotService.canonicalize(d);
        String b = ConfigSnapshotService.canonicalize(d);
        assertEquals(a, b);
        assertEquals(ConfigSnapshotDao.sha256(a),
                ConfigSnapshotDao.sha256(b));
    }

    /* =============================== redaction =============================== */

    @Test
    void sanitize_redacts_sensitive_keys() {
        Document d = new Document("username", "alice")
                .append("password", "s3cret")
                .append("authToken", "abc")
                .append("apiKey", "xyz")
                .append("other", "ok");
        Document out = ConfigSnapshotService.sanitize(d);
        assertEquals("alice", out.getString("username"));
        assertEquals("<redacted>", out.getString("password"));
        assertEquals("<redacted>", out.getString("authToken"));
        assertEquals("<redacted>", out.getString("apiKey"));
        assertEquals("ok", out.getString("other"));
    }

    @Test
    void redactCmdLine_handles_nested_docs() {
        Document d = new Document("parsed", new Document()
                .append("security", new Document("keyFile", "/etc/mongo.key")
                        .append("clusterAuthMode", "keyFile")));
        Document out = ConfigSnapshotService.redactCmdLine(d);
        Document sec = out.get("parsed", Document.class)
                .get("security", Document.class);
        assertEquals("<redacted>", sec.getString("keyFile"));
        assertEquals("keyFile", sec.getString("clusterAuthMode"));
    }

    /* ============================== DAO roundtrip ============================== */

    @Test
    void dao_round_trips_snapshot_and_hash() {
        String json = ConfigSnapshotService.canonicalize(
                new Document("foo", 1));
        String sha = ConfigSnapshotDao.sha256(json);
        ConfigSnapshot snap = new ConfigSnapshot(-1, "cx-1", 1_700_000L,
                "h1:27017", ConfigSnapshot.Kind.PARAMETERS, json, sha);
        long id = dao.insert(snap);
        assertTrue(id > 0);

        var latest = dao.latest("cx-1", "h1:27017",
                ConfigSnapshot.Kind.PARAMETERS);
        assertTrue(latest.isPresent());
        assertEquals(sha, latest.get().sha256());
    }

    @Test
    void listForConnection_orders_desc_by_capture_time() {
        dao.insert(new ConfigSnapshot(-1, "cx-1", 100L, null,
                ConfigSnapshot.Kind.FCV, "{\"v\":1}", "aa"));
        dao.insert(new ConfigSnapshot(-1, "cx-1", 200L, null,
                ConfigSnapshot.Kind.FCV, "{\"v\":2}", "bb"));
        var rows = dao.listForConnection("cx-1", 5);
        assertEquals(200L, rows.get(0).capturedAt());
        assertEquals(100L, rows.get(1).capturedAt());
    }
}
