package com.kubrik.mex.security.audit;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-C2 — parser fixtures covering the JSON shapes MongoDB 4.x → 7.x
 * emit for the common audit atypes. Malformed + missing-field inputs must
 * return null so the tailer can skip cleanly.
 */
class AuditEventParserTest {

    @Test
    void parses_successful_authenticate_line() {
        String line = """
                {"atype":"authenticate","ts":{"$date":"2026-04-21T10:00:00Z"},
                 "local":{"ip":"127.0.0.1","port":27017},
                 "remote":{"ip":"10.0.0.5","port":55233},
                 "users":[{"user":"dba","db":"admin"}],
                 "roles":[{"role":"root","db":"admin"}],
                 "param":{"user":"dba","db":"admin","mechanism":"SCRAM-SHA-256"},
                 "result":0}
                """.replace("\n", "");

        AuditEvent e = AuditEventParser.parse(line);
        assertNotNull(e);
        assertEquals("authenticate", e.atype());
        assertEquals("dba", e.who());
        assertEquals("admin", e.whoDb());
        assertEquals("10.0.0.5:55233", e.fromHost());
        assertEquals(0, e.result());
        assertEquals("SCRAM-SHA-256", e.param().get("mechanism"));
        assertEquals(java.time.Instant.parse("2026-04-21T10:00:00Z").toEpochMilli(),
                e.tsMs());
    }

    @Test
    void parses_numeric_ts_from_older_servers() {
        String line = "{\"atype\":\"authenticate\",\"ts\":1700000000000,"
                + "\"users\":[],\"param\":{}}";
        AuditEvent e = AuditEventParser.parse(line);
        assertNotNull(e);
        assertEquals(1_700_000_000_000L, e.tsMs());
    }

    @Test
    void rejects_invalid_json_without_crashing() {
        assertNull(AuditEventParser.parse("{not json"));
        assertNull(AuditEventParser.parse(""));
        assertNull(AuditEventParser.parse(null));
    }

    @Test
    void rejects_lines_without_atype() {
        assertNull(AuditEventParser.parse("{\"ts\":1700000000000,\"users\":[]}"));
    }

    @Test
    void missing_users_array_yields_empty_who_but_still_a_valid_event() {
        String line = "{\"atype\":\"logout\",\"ts\":0,\"param\":{}}";
        AuditEvent e = AuditEventParser.parse(line);
        assertNotNull(e);
        assertEquals("", e.who());
        assertEquals("", e.whoDb());
    }

    @Test
    void param_subdocuments_are_rendered_into_flat_map_values() {
        String line = "{\"atype\":\"createRole\",\"ts\":0,\"users\":[],"
                + "\"param\":{\"role\":\"appOps\","
                + "\"privileges\":[{\"resource\":{\"db\":\"shop\",\"collection\":\"\"},"
                + "\"actions\":[\"find\"]}]}}";
        AuditEvent e = AuditEventParser.parse(line);
        assertNotNull(e);
        assertEquals("appOps", e.param().get("role"));
        // Sub-arrays / sub-docs become searchable string values in FTS.
        Object privs = e.param().get("privileges");
        assertNotNull(privs);
        assertTrue(privs.toString().contains("shop"));
    }

    @Test
    void raw_json_survives_on_the_event_for_fts_indexing() {
        String line = "{\"atype\":\"createUser\",\"ts\":0,\"users\":[],\"param\":{}}";
        AuditEvent e = AuditEventParser.parse(line);
        assertNotNull(e);
        assertEquals(line, e.rawJson());
    }

    @Test
    void result_field_coerces_through_Number_to_int() {
        String line = "{\"atype\":\"authenticate\",\"ts\":0,\"users\":[],"
                + "\"param\":{},\"result\":18}";
        AuditEvent e = AuditEventParser.parse(line);
        assertEquals(18, e.result());
    }
}
