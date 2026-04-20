package com.kubrik.mex.monitoring.sampler;

import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.monitoring.model.LabelSet;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.util.DocUtil;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** OP-* via {@code $currentOp}. */
public final class CurrentOpSampler implements Sampler {

    private final String connectionId;
    private final MongoService svc;

    public CurrentOpSampler(String connectionId, MongoService svc) {
        this.connectionId = connectionId;
        this.svc = svc;
    }

    @Override public SamplerKind kind() { return SamplerKind.CURRENT_OP; }
    @Override public String connectionId() { return connectionId; }

    @Override
    public List<MetricSample> sample(Instant now) {
        long ts = now.toEpochMilli();
        List<MetricSample> out = new ArrayList<>();
        try {
            MongoDatabase admin = svc.database("admin");
            AggregateIterable<Document> it = admin.aggregate(List.of(
                    new Document("$currentOp", new Document("allUsers", true).append("idleSessions", false))
            ));
            int active = 0, waitingLock = 0, prepareConflict = 0;
            long longest = 0;
            Map<String, Integer> byOp = new HashMap<>();
            for (Document op : it) {
                active++;
                String opType = op.getString("op");
                if (opType != null) byOp.merge(opType, 1, Integer::sum);
                long secs = DocUtil.longVal(op, "secs_running", 0);
                if (secs > longest) longest = secs;
                if (Boolean.TRUE.equals(op.getBoolean("waitingForLock"))) waitingLock++;
                if (DocUtil.longVal(op, "prepareReadConflicts", 0) > 0) prepareConflict++;
            }
            emit(out, MetricId.OP_1, LabelSet.EMPTY, ts, active);
            emit(out, MetricId.OP_2, LabelSet.EMPTY, ts, longest);
            emit(out, MetricId.OP_3, LabelSet.EMPTY, ts, waitingLock);
            emit(out, MetricId.OP_4, LabelSet.EMPTY, ts, prepareConflict);
            for (var e : byOp.entrySet()) {
                emit(out, MetricId.OP_5, LabelSet.of("op_type", e.getKey()), ts, e.getValue());
            }
        } catch (Throwable ignored) {}
        return out;
    }

    private void emit(List<MetricSample> out, MetricId id, LabelSet l, long ts, double v) {
        out.add(new MetricSample(connectionId, id, l, ts, v));
    }
}
