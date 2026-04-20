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

/** TOP-* — per-namespace read/write lock time. Primary-only. */
public final class TopSampler implements Sampler {

    private final String connectionId;
    private final MongoService svc;
    private final CounterTracker readCounters  = new CounterTracker();
    private final CounterTracker writeCounters = new CounterTracker();
    private final CounterTracker totalCounters = new CounterTracker();
    private final CounterTracker readOps       = new CounterTracker();
    private final CounterTracker writeOps      = new CounterTracker();

    public TopSampler(String connectionId, MongoService svc) {
        this.connectionId = connectionId;
        this.svc = svc;
    }

    @Override public SamplerKind kind() { return SamplerKind.TOP; }
    @Override public String connectionId() { return connectionId; }

    @Override
    public List<MetricSample> sample(Instant now) {
        long ts = now.toEpochMilli();
        List<MetricSample> out = new ArrayList<>();
        Document resp;
        try { resp = svc.database("admin").runCommand(new Document("top", 1)); }
        catch (Throwable t) { return out; }
        Document totals = DocUtil.sub(resp, "totals");
        for (String ns : totals.keySet()) {
            if (ns.equals("note")) continue;
            Document t = DocUtil.sub(totals, ns);
            if (t.isEmpty()) continue;
            int dot = ns.indexOf('.');
            if (dot < 0) continue;
            LabelSet lbl = LabelSet.of("db", ns.substring(0, dot), "coll", ns.substring(dot + 1));
            Document rd = DocUtil.sub(t, "readLock");
            Document wr = DocUtil.sub(t, "writeLock");
            Document tl = DocUtil.sub(t, "total");
            readCounters.rate(connectionId, MetricId.TOP_1, lbl, ts, DocUtil.doubleVal(rd, "time", 0))
                    .ifPresent(v -> emit(out, MetricId.TOP_1, lbl, ts, v / 1000.0));
            writeCounters.rate(connectionId, MetricId.TOP_2, lbl, ts, DocUtil.doubleVal(wr, "time", 0))
                    .ifPresent(v -> emit(out, MetricId.TOP_2, lbl, ts, v / 1000.0));
            totalCounters.rate(connectionId, MetricId.TOP_3, lbl, ts, DocUtil.doubleVal(tl, "time", 0))
                    .ifPresent(v -> emit(out, MetricId.TOP_3, lbl, ts, v / 1000.0));
            var rOps = readOps.rate(connectionId, MetricId.TOP_4, lbl, ts,
                    DocUtil.doubleVal(rd, "count", 0));
            var wOps = writeOps.rate(connectionId, MetricId.TOP_5, lbl, ts,
                    DocUtil.doubleVal(wr, "count", 0));
            rOps.ifPresent(v -> emit(out, MetricId.TOP_4, lbl, ts, v));
            wOps.ifPresent(v -> emit(out, MetricId.TOP_5, lbl, ts, v));
        }
        return out;
    }

    private void emit(List<MetricSample> out, MetricId id, LabelSet l, long ts, double v) {
        out.add(new MetricSample(connectionId, id, l, ts, v));
    }
}
