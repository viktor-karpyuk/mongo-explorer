package com.kubrik.mex.monitoring.sampler;

import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.monitoring.model.LabelSet;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.util.DocUtil;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Produces INST-*, LAT-*, WT-*, LOCK-*, NET-*, CUR-*, TXN-*, ASRT-* every poll
 * from a single {@code serverStatus} command. See requirements.md §2.1–§2.12 and
 * technical-spec §4.
 *
 * <p>Cumulative counters (ops, asserts, network bytes) go through {@link CounterTracker};
 * the first poll per key returns no rate (absent from the output batch).
 *
 * <p>Read from {@link Document} using {@link DocUtil} helpers that tolerate missing
 * fields and numeric-vs-long surprises across server versions.
 */
public final class ServerStatusSampler implements Sampler {

    private final String connectionId;
    private final LabelSet hostLabels;
    private final ServerStatusFetcher fetcher;
    private final CounterTracker counters;
    private final LatencyTracker latencies;

    public ServerStatusSampler(String connectionId, String host, ServerStatusFetcher fetcher) {
        this.connectionId = connectionId;
        this.hostLabels = host == null || host.isEmpty() ? LabelSet.EMPTY : LabelSet.of("host", host);
        this.fetcher = fetcher;
        this.counters = new CounterTracker();
        this.latencies = new LatencyTracker();
    }

    /** Production factory — binds to a live {@link MongoService}. */
    public static ServerStatusSampler forService(String connectionId, MongoService svc) {
        String host;
        try { host = svc.hello().getString("me"); }
        catch (Throwable t) { host = null; }
        return new ServerStatusSampler(connectionId, host,
                () -> svc.database("admin").runCommand(new Document("serverStatus", 1)));
    }

    @Override public SamplerKind kind() { return SamplerKind.SERVER_STATUS; }
    @Override public String connectionId() { return connectionId; }

    @Override
    public List<MetricSample> sample(Instant now) throws Exception {
        Document doc = fetcher.fetch();
        long ts = now.toEpochMilli();
        List<MetricSample> out = new ArrayList<>(96);

        // 2.1 Throughput (counters — emit only once a baseline exists)
        Document op = DocUtil.sub(doc, "opcounters");
        rate(out, MetricId.INST_OP_1, ts, op, "insert");
        rate(out, MetricId.INST_OP_2, ts, op, "query");
        rate(out, MetricId.INST_OP_3, ts, op, "update");
        rate(out, MetricId.INST_OP_4, ts, op, "delete");
        rate(out, MetricId.INST_OP_5, ts, op, "getmore");
        rate(out, MetricId.INST_OP_6, ts, op, "command");

        Document opRepl = DocUtil.sub(doc, "opcountersRepl");
        rate(out, MetricId.INST_OP_7, ts, opRepl, "insert");
        rate(out, MetricId.INST_OP_8, ts, opRepl, "update");
        rate(out, MetricId.INST_OP_9, ts, opRepl, "delete");

        // 2.2 Latency — avg derived from cumulative (totalMicros, ops) pair
        Document lat = DocUtil.sub(doc, "opLatencies");
        avgLatency(out, MetricId.LAT_1, ts, DocUtil.sub(lat, "reads"));
        avgLatency(out, MetricId.LAT_2, ts, DocUtil.sub(lat, "writes"));
        avgLatency(out, MetricId.LAT_3, ts, DocUtil.sub(lat, "commands"));
        avgLatency(out, MetricId.LAT_4, ts, DocUtil.sub(lat, "transactions"));

        // 2.3 Connections
        Document conn = DocUtil.sub(doc, "connections");
        if (!conn.isEmpty()) {
            long current   = DocUtil.longVal(conn, "current", 0);
            long available = DocUtil.longVal(conn, "available", 0);
            emit(out, MetricId.INST_CONN_1, ts, current);
            emit(out, MetricId.INST_CONN_2, ts, available);
            emit(out, MetricId.INST_CONN_3, ts, DocUtil.longVal(conn, "active", 0));
            double sat = (current + available) == 0 ? 0 : (double) current / (current + available);
            emit(out, MetricId.INST_CONN_5, ts, sat);
            rate(out, MetricId.INST_CONN_4, ts, conn, "totalCreated");
            if (conn.containsKey("threaded"))      emit(out, MetricId.INST_CONN_6, ts, DocUtil.longVal(conn, "threaded", 0));
            if (conn.containsKey("loadBalanced"))  emit(out, MetricId.INST_CONN_7, ts, DocUtil.longVal(conn, "loadBalanced", 0));
        }

        // 2.4 Memory
        Document mem = DocUtil.sub(doc, "mem");
        if (!mem.isEmpty()) {
            emit(out, MetricId.INST_MEM_1, ts, DocUtil.longVal(mem, "resident", 0) * 1024L * 1024L);
            emit(out, MetricId.INST_MEM_2, ts, DocUtil.longVal(mem, "virtual", 0) * 1024L * 1024L);
            if (mem.containsKey("mapped"))
                emit(out, MetricId.INST_MEM_3, ts, DocUtil.longVal(mem, "mapped", 0) * 1024L * 1024L);
            emit(out, MetricId.INST_MEM_4, ts, DocUtil.boolVal(mem, "supported", false) ? 1.0 : 0.0);
        }

        // 2.5 + 2.6 + 2.7 — WiredTiger
        Document wt = DocUtil.sub(doc, "wiredTiger");
        if (!wt.isEmpty()) emitWt(out, ts, wt);

        // 2.8 Global lock
        Document gl = DocUtil.sub(doc, "globalLock");
        Document glq = DocUtil.sub(gl, "currentQueue");
        if (!glq.isEmpty()) {
            emit(out, MetricId.LOCK_1, ts, DocUtil.longVal(glq, "readers", 0));
            emit(out, MetricId.LOCK_2, ts, DocUtil.longVal(glq, "writers", 0));
            emit(out, MetricId.LOCK_3, ts, DocUtil.longVal(glq, "total", 0));
        }
        Document gla = DocUtil.sub(gl, "activeClients");
        if (!gla.isEmpty()) {
            emit(out, MetricId.LOCK_4, ts, DocUtil.longVal(gla, "readers", 0));
            emit(out, MetricId.LOCK_5, ts, DocUtil.longVal(gla, "writers", 0));
        }

        // 2.9 Network
        Document net = DocUtil.sub(doc, "network");
        rate(out, MetricId.NET_1, ts, net, "bytesIn");
        rate(out, MetricId.NET_2, ts, net, "bytesOut");
        rate(out, MetricId.NET_3, ts, net, "numRequests");

        // 2.10 Cursors
        Document cur = DocUtil.sub(DocUtil.sub(doc, "metrics"), "cursor");
        Document curOpen = DocUtil.sub(cur, "open");
        if (!curOpen.isEmpty()) {
            emit(out, MetricId.CUR_1, ts, DocUtil.longVal(curOpen, "total", 0));
            emit(out, MetricId.CUR_2, ts, DocUtil.longVal(curOpen, "noTimeout", 0));
            emit(out, MetricId.CUR_3, ts, DocUtil.longVal(curOpen, "pinned", 0));
        }
        rate(out, MetricId.CUR_4, ts, cur, "timedOut");

        // 2.11 Transactions
        Document txn = DocUtil.sub(doc, "transactions");
        if (!txn.isEmpty()) {
            emit(out, MetricId.TXN_1, ts, DocUtil.longVal(txn, "currentActive", 0));
            emit(out, MetricId.TXN_2, ts, DocUtil.longVal(txn, "currentInactive", 0));
            emit(out, MetricId.TXN_3, ts, DocUtil.longVal(txn, "currentOpen", 0));
            rate(out, MetricId.TXN_4, ts, txn, "totalCommitted");
            rate(out, MetricId.TXN_5, ts, txn, "totalAborted");
            rate(out, MetricId.TXN_7, ts, txn, "totalPrepared");
            rate(out, MetricId.TXN_8, ts, txn, "retriedCommandsCount");
            long committed = DocUtil.longVal(txn, "totalCommitted", 0);
            long aborted   = DocUtil.longVal(txn, "totalAborted", 0);
            long total     = committed + aborted;
            if (total > 0) {
                double abortRate = aborted == 0 ? 0.0 : (double) aborted / total;
                emit(out, MetricId.TXN_6, ts, abortRate);
            }
        }

        // 2.12 Asserts
        Document asrt = DocUtil.sub(doc, "asserts");
        rate(out, MetricId.ASRT_1, ts, asrt, "regular");
        rate(out, MetricId.ASRT_2, ts, asrt, "warning");
        rate(out, MetricId.ASRT_3, ts, asrt, "msg");
        rate(out, MetricId.ASRT_4, ts, asrt, "user");
        rate(out, MetricId.ASRT_5, ts, asrt, "rollovers");

        return out;
    }

    private void emitWt(List<MetricSample> out, long ts, Document wt) {
        Document cache = DocUtil.sub(wt, "cache");
        if (!cache.isEmpty()) {
            long inCache = DocUtil.longVal(cache, "bytes currently in the cache", 0);
            long maxBytes = DocUtil.longVal(cache, "maximum bytes configured", 0);
            long dirty = DocUtil.longVal(cache, "tracked dirty bytes in the cache", 0);
            emit(out, MetricId.WT_1, ts, inCache);
            emit(out, MetricId.WT_2, ts, maxBytes);
            if (maxBytes > 0) emit(out, MetricId.WT_3, ts, (double) inCache / maxBytes);
            emit(out, MetricId.WT_4, ts, dirty);
            if (maxBytes > 0) emit(out, MetricId.WT_5, ts, (double) dirty / maxBytes);
            rate(out, MetricId.WT_6,  ts, cache, "pages read into cache");
            rate(out, MetricId.WT_7,  ts, cache, "pages written from cache");
            rate(out, MetricId.WT_8,  ts, cache, "eviction pages evicted");
            rate(out, MetricId.WT_9,  ts, cache, "worker thread evicting pages");
            rate(out, MetricId.WT_10, ts, cache,
                    "application threads page read from disk to cache count");
            rate(out, MetricId.WT_12, ts, cache, "unmodified pages evicted");
            rate(out, MetricId.WT_13, ts, cache, "modified pages evicted");
            // Hit-ratio: cheap approximation 1 - (reads / requested). "pages requested from the cache"
            // is the commonly-used denominator where present.
            double requested = DocUtil.doubleVal(cache, "pages requested from the cache", 0);
            double reads = DocUtil.doubleVal(cache, "pages read into cache", 0);
            if (requested > 0) emit(out, MetricId.WT_11, ts, Math.max(0.0, 1.0 - reads / requested));
        }

        Document tx = DocUtil.sub(wt, "concurrentTransactions");
        Document read = DocUtil.sub(tx, "read");
        Document write = DocUtil.sub(tx, "write");
        if (!read.isEmpty()) {
            long out_ = DocUtil.longVal(read, "out", 0);
            long avail = DocUtil.longVal(read, "available", 0);
            long total = DocUtil.longVal(read, "totalTickets", out_ + avail);
            emit(out, MetricId.WT_TKT_1, ts, out_);
            emit(out, MetricId.WT_TKT_2, ts, avail);
            emit(out, MetricId.WT_TKT_3, ts, total);
            if (total > 0) emit(out, MetricId.WT_TKT_4, ts, (double) out_ / total);
        }
        if (!write.isEmpty()) {
            long out_ = DocUtil.longVal(write, "out", 0);
            long avail = DocUtil.longVal(write, "available", 0);
            long total = DocUtil.longVal(write, "totalTickets", out_ + avail);
            emit(out, MetricId.WT_TKT_5, ts, out_);
            emit(out, MetricId.WT_TKT_6, ts, avail);
            emit(out, MetricId.WT_TKT_7, ts, total);
            if (total > 0) emit(out, MetricId.WT_TKT_8, ts, (double) out_ / total);
        }

        Document ckp = DocUtil.sub(wt, "checkpoint");
        if (!ckp.isEmpty()) {
            emit(out, MetricId.WT_CKP_1, ts, DocUtil.longVal(ckp, "most recent time (msecs)", 0));
            emit(out, MetricId.WT_CKP_3, ts, DocUtil.longVal(ckp, "max time (msecs)", 0));
            emit(out, MetricId.WT_CKP_4, ts, DocUtil.longVal(ckp, "scrub time (msecs)", 0));
        }
    }

    private void avgLatency(List<MetricSample> out, MetricId id, long ts, Document latBucket) {
        if (latBucket.isEmpty()) return;
        long latency = DocUtil.longVal(latBucket, "latency", 0);
        long ops = DocUtil.longVal(latBucket, "ops", 0);
        latencies.avgLatencyUs(id, latency, ops)
                .ifPresent(v -> emit(out, id, ts, v));
    }

    private void rate(List<MetricSample> out, MetricId id, long ts, Document d, String field) {
        if (!d.containsKey(field)) return;
        double cumulative = DocUtil.doubleVal(d, field, 0);
        counters.rate(connectionId, id, hostLabels, ts, cumulative)
                .ifPresent(v -> emit(out, id, ts, v));
    }

    private void emit(List<MetricSample> out, MetricId id, long ts, double value) {
        out.add(new MetricSample(connectionId, id, hostLabels, ts, value));
    }

    /** Swappable fetch call so unit tests can hand in a canned {@link Document}. */
    @FunctionalInterface
    public interface ServerStatusFetcher {
        Document fetch() throws Exception;
    }
}
