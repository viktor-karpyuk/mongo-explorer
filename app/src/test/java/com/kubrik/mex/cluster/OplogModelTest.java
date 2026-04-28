package com.kubrik.mex.cluster;

import com.kubrik.mex.cluster.ops.OplogEntry;
import com.kubrik.mex.cluster.ops.OplogGaugeStats;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.4 OPLOG-1..8 — parsing + band classification coverage. Live cluster
 * reads are covered by the topology / testcontainers suite.
 */
class OplogModelTest {

    @Test
    void entry_parses_op_ns_ts_and_preview() {
        Document raw = new Document("ts", new BsonTimestamp((int) 1_710_000_000L, 0))
                .append("op", "u")
                .append("ns", "reports.daily")
                .append("o", new Document("$set", new Document("count", 12)));
        OplogEntry e = OplogEntry.fromRaw(raw);
        assertEquals(1_710_000_000L, e.tsSec());
        assertEquals("u", e.op());
        assertEquals("update", e.opLabel());
        assertEquals("reports.daily", e.ns());
        assertTrue(e.preview().contains("count"));
    }

    @Test
    void gauge_band_follows_window_hours() {
        assertEquals("red",   statsWithHours(2.5).band());
        assertEquals("amber", statsWithHours(12).band());
        assertEquals("green", statsWithHours(36).band());
    }

    @Test
    void gauge_reports_usage_ratio() {
        OplogGaugeStats s = new OplogGaugeStats(true, 1_000L, 250L, 1L, 2L, 24, null);
        assertEquals(0.25, s.usageRatio(), 0.0001);
    }

    private static OplogGaugeStats statsWithHours(double hours) {
        return new OplogGaugeStats(true, 1_000L, 500L, 1L, 2L, hours, null);
    }
}
