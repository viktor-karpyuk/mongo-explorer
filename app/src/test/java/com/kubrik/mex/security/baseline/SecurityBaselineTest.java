package com.kubrik.mex.security.baseline;

import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-A3 — baseline record + DAO round-trip. Canonical JSON + SHA-256
 * must be deterministic (structurally-equal payloads → equal hash) and
 * order-independent for map keys.
 */
class SecurityBaselineTest {

    @TempDir Path home;
    private Database db;
    private SecurityBaselineDao dao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", home.toString());
        db = new Database();
        dao = new SecurityBaselineDao(db);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void equal_payloads_hash_equally_regardless_of_insertion_order() {
        // Same content, different insertion order — canonical JSON sorts
        // keys recursively so the SHA-256 should match.
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("users", Map.of("dba", Map.of("roles", java.util.List.of("root"))));
        a.put("params", Map.of("auditDestination", "file"));

        Map<String, Object> b = new LinkedHashMap<>();
        b.put("params", Map.of("auditDestination", "file"));
        b.put("users", Map.of("dba", Map.of("roles", java.util.List.of("root"))));

        SecurityBaseline left = new SecurityBaseline("1", "cx-a", 1000L, "dba", "", a);
        SecurityBaseline right = new SecurityBaseline("1", "cx-a", 1000L, "dba", "", b);

        assertEquals(left.sha256(), right.sha256(),
                "equal payloads in different insertion orders must hash equally");
        assertEquals(left.canonicalJson(), right.canonicalJson());
    }

    @Test
    void changing_one_value_changes_the_hash() {
        Map<String, Object> a = Map.of("users", Map.of("dba", Map.of("roles",
                java.util.List.of("root"))));
        Map<String, Object> b = Map.of("users", Map.of("dba", Map.of("roles",
                java.util.List.of("readWrite"))));

        SecurityBaseline left = new SecurityBaseline("1", "cx-a", 1000L, "dba", "", a);
        SecurityBaseline right = new SecurityBaseline("1", "cx-a", 1000L, "dba", "", b);

        assertNotEquals(left.sha256(), right.sha256());
    }

    @Test
    void dao_round_trip_preserves_hash_and_payload_json() {
        SecurityBaseline b = new SecurityBaseline("1", "cx-a", 1000L, "dba",
                "pre-release snapshot",
                Map.of("params", Map.of("auditDestination", "file")));
        SecurityBaselineDao.Row inserted = dao.insert(b);

        assertTrue(inserted.id() > 0);
        assertEquals(b.sha256(), inserted.sha256());
        assertEquals(b.canonicalJson(), inserted.snapshotJson());

        SecurityBaselineDao.Row loaded = dao.byId(inserted.id()).orElseThrow();
        assertEquals(inserted.sha256(), loaded.sha256());
        assertEquals(inserted.snapshotJson(), loaded.snapshotJson());
        assertEquals("pre-release snapshot", loaded.notes());
    }

    @Test
    void latest_for_connection_returns_the_newest_row() {
        dao.insert(new SecurityBaseline("1", "cx-a", 1000L, "dba", "", Map.of("k", 1)));
        dao.insert(new SecurityBaseline("1", "cx-a", 2000L, "dba", "", Map.of("k", 2)));
        dao.insert(new SecurityBaseline("1", "cx-a", 3000L, "dba", "", Map.of("k", 3)));

        SecurityBaselineDao.Row latest = dao.latestForConnection("cx-a").orElseThrow();
        assertEquals(3000L, latest.capturedAt());
    }

    @Test
    void connectionId_is_required() {
        assertThrows(IllegalArgumentException.class, () ->
                new SecurityBaseline("1", null, 0L, "dba", "", Map.of()));
        assertThrows(IllegalArgumentException.class, () ->
                new SecurityBaseline("1", "  ", 0L, "dba", "", Map.of()));
    }
}
