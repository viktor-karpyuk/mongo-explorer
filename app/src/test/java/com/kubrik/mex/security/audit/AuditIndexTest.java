package com.kubrik.mex.security.audit;

import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-C3 — FTS5 round-trip. Confirms the virtual table was created
 * by the migration, that insert + search + recent + purge work as the
 * pane needs them to, and that connection_id isolation is honoured.
 */
class AuditIndexTest {

    @TempDir Path home;
    private Database db;
    private AuditIndex index;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", home.toString());
        db = new Database();
        index = new AuditIndex(db);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void insert_then_listRecent_returns_the_event() {
        index.insert("cx-a", event("authenticate", 1_000L, "dba", "admin", "127.0.0.1"));
        List<AuditEvent> recent = index.listRecent("cx-a", 10);
        assertEquals(1, recent.size());
        AuditEvent e = recent.get(0);
        assertEquals("authenticate", e.atype());
        assertEquals(1_000L, e.tsMs());
        assertEquals("dba", e.who());
        assertEquals("admin", e.whoDb());
    }

    @Test
    void search_finds_by_atype_token() {
        index.insert("cx-a", event("authenticate", 1_000L, "dba", "admin", "h"));
        index.insert("cx-a", event("createUser", 2_000L, "dba", "admin", "h"));
        index.insert("cx-a", event("logout", 3_000L, "dba", "admin", "h"));

        List<AuditEvent> hits = index.search("cx-a", "authenticate", 10);
        assertEquals(1, hits.size());
        assertEquals("authenticate", hits.get(0).atype());
    }

    @Test
    void search_finds_by_who_field_with_column_scope() {
        index.insert("cx-a", event("authenticate", 1_000L, "dba", "admin", "h"));
        index.insert("cx-a", event("authenticate", 2_000L, "ops", "shop", "h"));

        List<AuditEvent> hits = index.search("cx-a", "who:dba", 10);
        assertEquals(1, hits.size());
        assertEquals("dba", hits.get(0).who());
    }

    @Test
    void connection_id_filter_prevents_cross_talk() {
        index.insert("cx-a", event("authenticate", 1_000L, "dba", "admin", "h"));
        index.insert("cx-b", event("authenticate", 2_000L, "dba", "admin", "h"));

        assertEquals(1, index.listRecent("cx-a", 10).size());
        assertEquals(1, index.listRecent("cx-b", 10).size());
        assertEquals(1, index.search("cx-a", "authenticate", 10).size());
    }

    @Test
    void listRecent_orders_newest_first() {
        index.insert("cx-a", event("a", 1_000L, "u", "admin", "h"));
        index.insert("cx-a", event("b", 3_000L, "u", "admin", "h"));
        index.insert("cx-a", event("c", 2_000L, "u", "admin", "h"));

        List<AuditEvent> recent = index.listRecent("cx-a", 10);
        assertEquals(3_000L, recent.get(0).tsMs());
        assertEquals(2_000L, recent.get(1).tsMs());
        assertEquals(1_000L, recent.get(2).tsMs());
    }

    @Test
    void purgeOlderThan_drops_events_before_the_cutoff() {
        index.insert("cx-a", event("old", 500L, "u", "admin", "h"));
        index.insert("cx-a", event("new", 5_000L, "u", "admin", "h"));

        int dropped = index.purgeOlderThan("cx-a", 1_000L);
        assertEquals(1, dropped);
        List<AuditEvent> remaining = index.listRecent("cx-a", 10);
        assertEquals(1, remaining.size());
        assertEquals("new", remaining.get(0).atype());
    }

    @Test
    void insert_with_null_inputs_is_a_safe_noop() {
        index.insert(null, event("x", 0, "u", "admin", "h"));
        index.insert("cx-a", null);
        assertTrue(index.listRecent("cx-a", 10).isEmpty());
    }

    @Test
    void empty_query_falls_back_to_atype_wildcard_match() {
        index.insert("cx-a", event("authenticate", 1_000L, "u", "admin", "h"));
        List<AuditEvent> hits = index.search("cx-a", "", 10);
        assertEquals(1, hits.size());
    }

    private static AuditEvent event(String atype, long ts, String who, String whoDb,
                                      String fromHost) {
        return new AuditEvent(atype, ts, who, whoDb, fromHost, 0,
                java.util.Map.of("param", "value"),
                "{\"atype\":\"" + atype + "\"}");
    }
}
