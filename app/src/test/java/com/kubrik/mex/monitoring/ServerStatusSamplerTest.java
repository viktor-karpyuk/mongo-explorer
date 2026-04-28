package com.kubrik.mex.monitoring;

import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.sampler.ServerStatusSampler;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Drives the parser with canned {@code serverStatus} Documents. */
class ServerStatusSamplerTest {

    @Test
    void rateMetricsAppearOnSecondPollOnly() throws Exception {
        Document ss1 = stubServerStatus(100, 1_000_000_000L, 200_000_000L, 0, 0, 0, 0, 0);
        Document ss2 = stubServerStatus(150, 1_000_000_000L, 220_000_000L, 0, 0, 0, 0, 0);
        ServerStatusSampler sampler = new ServerStatusSampler("c1", "host-0",
                new StubFetcher(List.of(ss1, ss2)));
        List<MetricSample> firstPoll = sampler.sample(Instant.ofEpochMilli(10_000));
        List<MetricSample> secondPoll = sampler.sample(Instant.ofEpochMilli(11_000));
        // First poll emits only non-rate gauges: WT cache fill, dirty ratio, connections,
        // tickets — and must NOT emit INST_OP_1 (rate).
        assertFalse(samplesContain(firstPoll, MetricId.INST_OP_1),
                "INST_OP_1 is a rate — first poll cannot emit it");
        assertTrue(samplesContain(secondPoll, MetricId.INST_OP_1),
                "second poll must produce an INST_OP_1 rate");
        double rate = firstValue(secondPoll, MetricId.INST_OP_1);
        assertEquals(50.0, rate, 1e-6, "(150 - 100) inserts over 1 s");
    }

    @Test
    void cacheFillRatioEmittedOnFirstPoll() throws Exception {
        // bytesInCache=200MB, max=1GB → ratio 0.1953125
        Document ss = stubServerStatus(0, 1_073_741_824L, 210_000_000L, 0, 0, 0, 0, 0);
        ServerStatusSampler sampler = new ServerStatusSampler("c1", "host-0",
                new StubFetcher(List.of(ss)));
        List<MetricSample> out = sampler.sample(Instant.ofEpochMilli(1_000));
        assertTrue(samplesContain(out, MetricId.WT_3), "cache fill is a gauge — first poll emits it");
        double v = firstValue(out, MetricId.WT_3);
        assertTrue(v > 0.19 && v < 0.20, "expected ~0.195 got " + v);
    }

    @Test
    void avgLatencyRequiresTwoPollsWithOps() throws Exception {
        Document a = latencyOnly(100_000, 50);
        Document b = latencyOnly(400_000, 100);
        ServerStatusSampler sampler = new ServerStatusSampler("c1", "host-0",
                new StubFetcher(List.of(a, b)));
        sampler.sample(Instant.ofEpochMilli(1_000));
        List<MetricSample> second = sampler.sample(Instant.ofEpochMilli(2_000));
        double v = firstValue(second, MetricId.LAT_1);
        assertEquals(6000.0, v, 1e-6,
                "(400_000 - 100_000) µs over 50 ops = 6000 µs/op");
    }

    // ---------------- helpers ----------------

    private static boolean samplesContain(List<MetricSample> ss, MetricId id) {
        return ss.stream().anyMatch(s -> s.metric() == id);
    }

    private static double firstValue(List<MetricSample> ss, MetricId id) {
        return ss.stream().filter(s -> s.metric() == id).findFirst().orElseThrow().value();
    }

    private static Document stubServerStatus(long opInsert,
                                             long wtMax, long wtBytes, long wtDirty,
                                             long connCur, long connAvail, long connActive,
                                             long connTotalCreated) {
        Document cache = new Document()
                .append("bytes currently in the cache", wtBytes)
                .append("maximum bytes configured", wtMax)
                .append("tracked dirty bytes in the cache", wtDirty)
                .append("pages read into cache", 0)
                .append("pages written from cache", 0)
                .append("eviction pages evicted", 0)
                .append("worker thread evicting pages", 0)
                .append("application threads page read from disk to cache count", 0)
                .append("unmodified pages evicted", 0)
                .append("modified pages evicted", 0);
        Document wt = new Document()
                .append("cache", cache)
                .append("concurrentTransactions", new Document()
                        .append("read",  new Document().append("out", 0).append("available", 128).append("totalTickets", 128))
                        .append("write", new Document().append("out", 0).append("available", 128).append("totalTickets", 128)))
                .append("checkpoint", new Document()
                        .append("most recent time (msecs)", 10)
                        .append("max time (msecs)", 20)
                        .append("scrub time (msecs)", 0));
        Document doc = new Document()
                .append("opcounters", new Document()
                        .append("insert", opInsert).append("query", 0).append("update", 0)
                        .append("delete", 0).append("getmore", 0).append("command", 0))
                .append("opLatencies", new Document()
                        .append("reads",        new Document().append("latency", 0).append("ops", 0))
                        .append("writes",       new Document().append("latency", 0).append("ops", 0))
                        .append("commands",     new Document().append("latency", 0).append("ops", 0)))
                .append("connections", new Document()
                        .append("current", connCur).append("available", connAvail)
                        .append("active", connActive).append("totalCreated", connTotalCreated))
                .append("mem", new Document()
                        .append("resident", 1).append("virtual", 2).append("supported", true))
                .append("wiredTiger", wt)
                .append("globalLock", new Document()
                        .append("currentQueue", new Document().append("readers", 0).append("writers", 0).append("total", 0))
                        .append("activeClients", new Document().append("readers", 0).append("writers", 0)))
                .append("network", new Document().append("bytesIn", 0).append("bytesOut", 0).append("numRequests", 0))
                .append("metrics", new Document().append("cursor", new Document()
                        .append("open", new Document().append("total", 0).append("noTimeout", 0).append("pinned", 0))
                        .append("timedOut", 0)))
                .append("transactions", new Document()
                        .append("currentActive", 0).append("currentInactive", 0).append("currentOpen", 0)
                        .append("totalCommitted", 0).append("totalAborted", 0))
                .append("asserts", new Document()
                        .append("regular", 0).append("warning", 0).append("msg", 0)
                        .append("user", 0).append("rollovers", 0));
        return doc;
    }

    private static Document latencyOnly(long latencyUs, long ops) {
        Document empty = stubServerStatus(0, 0, 0, 0, 0, 0, 0, 0);
        Document lat = (Document) empty.get("opLatencies");
        Document reads = (Document) lat.get("reads");
        reads.put("latency", latencyUs);
        reads.put("ops", ops);
        return empty;
    }

    private static final class StubFetcher implements ServerStatusSampler.ServerStatusFetcher {
        private final java.util.Iterator<Document> it;
        StubFetcher(List<Document> docs) { this.it = docs.iterator(); }
        @Override public Document fetch() { return it.next(); }
    }
}
