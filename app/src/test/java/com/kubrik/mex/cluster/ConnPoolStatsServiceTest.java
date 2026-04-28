package com.kubrik.mex.cluster;

import com.kubrik.mex.cluster.ops.ConnPoolStats;
import com.kubrik.mex.cluster.service.ConnPoolStatsService;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.4 POOL-1..5 — parses {@code connPoolStats}. Verifies the hosts
 * sub-document decomposition, total roll-up fields, and that a zero-length
 * response parses to an empty record without throwing.
 */
class ConnPoolStatsServiceTest {

    @Test
    void parses_hosts_and_totals() {
        Document raw = new Document("totalInUse", 40)
                .append("totalAvailable", 210)
                .append("totalCreated", 250)
                .append("hosts", new Document()
                        .append("prod-rs-01:27018", new Document("poolSize", 100)
                                .append("inUse", 22).append("available", 78)
                                .append("created", 100).append("waitQueueSize", 0)
                                .append("timeouts", 0L))
                        .append("mongos-03:27017", new Document("poolSize", 50)
                                .append("inUse", 47).append("available", 3)
                                .append("created", 150).append("waitQueueSize", 9)
                                .append("timeouts", 2L)));
        ConnPoolStats s = ConnPoolStatsService.parse(raw);
        assertEquals(2, s.rows().size());
        assertEquals(40, s.totalInUse());
        assertEquals(210, s.totalAvailable());
        assertEquals(250, s.totalCreated());

        ConnPoolStats.Row hot = s.rows().stream()
                .filter(r -> r.host().equals("mongos-03:27017"))
                .findFirst().orElseThrow();
        assertEquals(9, hot.waitQueueSize());
        assertEquals(2L, hot.timeouts());
    }

    @Test
    void empty_payload_is_graceful() {
        ConnPoolStats s = ConnPoolStatsService.parse(new Document());
        assertTrue(s.rows().isEmpty());
        assertEquals(0, s.totalInUse());
    }
}
