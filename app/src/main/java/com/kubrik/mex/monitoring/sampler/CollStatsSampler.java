package com.kubrik.mex.monitoring.sampler;

import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.monitoring.model.LabelSet;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.util.DocUtil;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

/**
 * COLLSTAT-* + IDX-FOOT-* per monitored collection per poll. Cardinality is
 * capped to top-N-by-storageSize per DB with an aggregated "_other_" row
 * (see requirements.md §5.2).
 */
public final class CollStatsSampler implements Sampler {

    private final String connectionId;
    private final MongoService svc;
    private final Supplier<List<String>> dbNames;
    private final int topN;

    public CollStatsSampler(String connectionId, MongoService svc,
                            Supplier<List<String>> dbNames, int topN) {
        this.connectionId = connectionId;
        this.svc = svc;
        this.dbNames = dbNames;
        this.topN = topN;
    }

    @Override public SamplerKind kind() { return SamplerKind.COLL_STATS; }
    @Override public String connectionId() { return connectionId; }

    @Override
    public List<MetricSample> sample(Instant now) {
        long ts = now.toEpochMilli();
        List<MetricSample> out = new ArrayList<>();
        for (String db : dbNames.get()) {
            List<String> colls;
            try { colls = svc.listCollectionNames(db); }
            catch (Throwable t) { continue; }
            List<Document> stats = new ArrayList<>(colls.size());
            for (String c : colls) {
                try { stats.add(svc.collStats(db, c).append("__ns", db + "." + c).append("__coll", c)); }
                catch (Throwable ignored) {}
            }
            stats.sort(Comparator.comparingLong((Document d) ->
                    DocUtil.longVal(d, "storageSize", 0)).reversed());
            int emitted = 0;
            long otherCount = 0, otherSize = 0, otherStorage = 0, otherIndex = 0;
            for (Document s : stats) {
                String coll = s.getString("__coll");
                if (emitted < topN) {
                    emitCollRow(out, db, coll, s, ts);
                    emitted++;
                } else {
                    otherCount   += DocUtil.longVal(s, "count", 0);
                    otherSize    += DocUtil.longVal(s, "size", 0);
                    otherStorage += DocUtil.longVal(s, "storageSize", 0);
                    otherIndex   += DocUtil.longVal(s, "totalIndexSize", 0);
                }
            }
            if (emitted == topN && !stats.isEmpty() && stats.size() > topN) {
                LabelSet lbl = LabelSet.of("db", db, "coll", "_other_");
                emit(out, MetricId.COLLSTAT_1, lbl, ts, otherCount);
                emit(out, MetricId.COLLSTAT_2, lbl, ts, otherSize);
                emit(out, MetricId.COLLSTAT_4, lbl, ts, otherStorage);
                emit(out, MetricId.COLLSTAT_6, lbl, ts, otherIndex);
            }
        }
        return out;
    }

    private void emitCollRow(List<MetricSample> out, String db, String coll, Document s, long ts) {
        LabelSet lbl = LabelSet.of("db", db, "coll", coll);
        emit(out, MetricId.COLLSTAT_1, lbl, ts, DocUtil.longVal(s, "count", 0));
        emit(out, MetricId.COLLSTAT_2, lbl, ts, DocUtil.longVal(s, "size", 0));
        emit(out, MetricId.COLLSTAT_3, lbl, ts, DocUtil.doubleVal(s, "avgObjSize", 0));
        emit(out, MetricId.COLLSTAT_4, lbl, ts, DocUtil.longVal(s, "storageSize", 0));
        emit(out, MetricId.COLLSTAT_5, lbl, ts, DocUtil.longVal(s, "freeStorageSize", 0));
        emit(out, MetricId.COLLSTAT_6, lbl, ts, DocUtil.longVal(s, "totalIndexSize", 0));
        emit(out, MetricId.COLLSTAT_7, lbl, ts, DocUtil.longVal(s, "nindexes", 0));
        emit(out, MetricId.COLLSTAT_8, lbl, ts, DocUtil.boolVal(s, "capped", false) ? 1 : 0);
        // IDX-FOOT-* from indexSizes map
        Document idxSizes = DocUtil.sub(s, "indexSizes");
        for (String name : idxSizes.keySet()) {
            LabelSet ilbl = LabelSet.of("db", db, "coll", coll, "index", name);
            emit(out, MetricId.IDX_FOOT_1, ilbl, ts, DocUtil.doubleVal(idxSizes, name, 0));
        }
    }

    private void emit(List<MetricSample> out, MetricId id, LabelSet l, long ts, double v) {
        out.add(new MetricSample(connectionId, id, l, ts, v));
    }
}
