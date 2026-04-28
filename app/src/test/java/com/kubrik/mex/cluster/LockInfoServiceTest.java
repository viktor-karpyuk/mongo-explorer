package com.kubrik.mex.cluster;

import com.kubrik.mex.cluster.ops.LockInfo;
import com.kubrik.mex.cluster.service.LockInfoService;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.4 LOCK-1..4 — parsing coverage for {@code lockInfo}. Handles the
 * {@code granted / pending} payload shape plus the nested {@code resource}
 * document (type + ns) introduced in 4.2.
 */
class LockInfoServiceTest {

    @Test
    void parses_resource_entries_and_top_holders() {
        Document raw = new Document("lockInfo", List.of(
                new Document()
                        .append("resource", new Document("type", "Collection").append("ns", "reports.daily"))
                        .append("granted", List.of(
                                new Document("opid", 4917).append("microsHeld", 4_100_000L).append("mode", "IX"),
                                new Document("opid", 5001).append("microsHeld",   200_000L).append("mode", "IX")))
                        .append("pending", List.of(
                                new Document("opid", 5100).append("mode", "X"))),
                new Document()
                        .append("resource", new Document("type", "Global"))
                        .append("granted", List.of(
                                new Document("opid", 4917).append("microsHeld", 50_000L).append("mode", "r")))
                        .append("pending", List.of())
        ));
        LockInfo info = LockInfoService.parse(raw);
        assertTrue(info.supported());
        assertEquals(2, info.entries().size());

        LockInfo.Entry daily = info.entries().get(0);
        assertEquals("Collection:reports.daily", daily.resource());
        assertEquals(2, daily.holders());
        assertEquals(1, daily.waiters());
        assertEquals(4_100, daily.maxHoldMs());
        assertEquals("IX", daily.mode());

        assertEquals(3, info.topHolders().size());
        assertEquals(4917L, info.topHolders().get(0).opid(), "longest hold first");
    }

    @Test
    void returns_unsupported_when_field_absent() {
        LockInfo info = LockInfoService.parse(new Document("ok", 1));
        assertFalse(info.supported());
        assertTrue(info.entries().isEmpty());
    }
}
