package com.kubrik.mex.cluster;

import com.kubrik.mex.cluster.ops.CurrentOpRow;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.4 OP-1..6 — currentOp row parsing. Covers the field shapes returned by
 * 4.0+ servers: numeric opid, nested {@code effectiveUsers}, and comment
 * carried inside {@code command.$comment} vs. a top-level key.
 */
class CurrentOpRowTest {

    @Test
    void parses_happy_path() {
        Document raw = new Document()
                .append("opid", 4917L)
                .append("host", "prod-rs-01:27018")
                .append("ns", "reports.daily")
                .append("op", "command")
                .append("secs_running", 183L)
                .append("active", true)
                .append("planSummary", "IXSCAN { day: 1 }")
                .append("client", "10.0.0.7:55123")
                .append("effectiveUsers", List.of(new Document("user", "app-rw").append("db", "admin")))
                .append("command", new Document("aggregate", "daily").append("$comment", "nightly-rollup"));
        CurrentOpRow row = CurrentOpRow.fromRaw(raw);
        assertEquals(4917L, row.opid());
        assertEquals("prod-rs-01:27018", row.host());
        assertEquals("reports.daily", row.ns());
        assertEquals("command", row.op());
        assertEquals(183L, row.secsRunning());
        assertEquals("app-rw", row.user());
        assertEquals("nightly-rollup", row.comment());
        assertEquals("IXSCAN { day: 1 }", row.planSummary());
        assertFalse(row.waitingForLock());
    }

    @Test
    void tolerates_missing_optional_fields() {
        Document raw = new Document()
                .append("opid", 1)
                .append("op", "none");
        CurrentOpRow row = CurrentOpRow.fromRaw(raw);
        assertEquals(1L, row.opid());
        assertEquals("none", row.op());
        assertEquals("", row.host());
        assertEquals("", row.ns());
        assertEquals("", row.user());
        assertEquals("", row.comment());
    }

    @Test
    void reports_waiting_for_lock_with_reason() {
        Document raw = new Document()
                .append("opid", 42)
                .append("op", "update")
                .append("waitingForLock", true)
                .append("lockStats", new Document("Collection", new Document("acquireCount", new Document("w", 1L))));
        CurrentOpRow row = CurrentOpRow.fromRaw(raw);
        assertTrue(row.waitingForLock());
        assertEquals("Collection", row.waitingForLockReason());
    }
}
