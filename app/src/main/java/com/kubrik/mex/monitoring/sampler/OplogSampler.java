package com.kubrik.mex.monitoring.sampler;

import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.monitoring.model.LabelSet;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.util.DocUtil;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Sorts;
import org.bson.BsonTimestamp;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** REPL-OPLOG-* via {@code local.oplog.rs} stats + first/last timestamp. */
public final class OplogSampler implements Sampler {

    private final String connectionId;
    private final MongoService svc;
    private final CounterTracker counters = new CounterTracker();

    public OplogSampler(String connectionId, MongoService svc) {
        this.connectionId = connectionId;
        this.svc = svc;
    }

    @Override public SamplerKind kind() { return SamplerKind.OPLOG; }
    @Override public String connectionId() { return connectionId; }

    @Override
    public List<MetricSample> sample(Instant now) {
        long ts = now.toEpochMilli();
        List<MetricSample> out = new ArrayList<>();
        try {
            MongoCollection<Document> oplog = svc.database("local").getCollection("oplog.rs");
            Document stats;
            try { stats = svc.database("local").runCommand(new Document("collStats", "oplog.rs")); }
            catch (Throwable t) { return out; }

            long sizeBytes = DocUtil.longVal(stats, "maxSize",
                    DocUtil.longVal(stats, "storageSize", 0));
            long usedBytes = DocUtil.longVal(stats, "size", 0);
            emit(out, MetricId.REPL_OPLOG_1, ts, sizeBytes);
            emit(out, MetricId.REPL_OPLOG_2, ts, usedBytes);

            Document first = oplog.find().sort(Sorts.ascending("$natural")).limit(1).first();
            Document last  = oplog.find().sort(Sorts.descending("$natural")).limit(1).first();
            if (first != null && last != null) {
                BsonTimestamp ft = first.get("ts", BsonTimestamp.class);
                BsonTimestamp lt = last.get("ts", BsonTimestamp.class);
                if (ft != null && lt != null) {
                    double hours = (lt.getTime() - ft.getTime()) / 3600.0;
                    emit(out, MetricId.REPL_OPLOG_3, ts, hours);
                }
            }
            // Rates come from persisted server counters — not directly from oplog.
            // Leave 4/5 to ServerStatusSampler's INST-OP-7..9 composition for now.
        } catch (Throwable ignored) {
            // oplog absent → standalone; return what we've accumulated.
        }
        return out;
    }

    private void emit(List<MetricSample> out, MetricId id, long ts, double v) {
        out.add(new MetricSample(connectionId, id, LabelSet.EMPTY, ts, v));
    }
}
