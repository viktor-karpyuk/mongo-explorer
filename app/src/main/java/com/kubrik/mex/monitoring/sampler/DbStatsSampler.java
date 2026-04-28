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
import java.util.function.Supplier;

/** DBSTAT-* — one row per monitored database per poll. */
public final class DbStatsSampler implements Sampler {

    private final String connectionId;
    private final MongoService svc;
    private final Supplier<List<String>> dbNames;

    public DbStatsSampler(String connectionId, MongoService svc, Supplier<List<String>> dbNames) {
        this.connectionId = connectionId;
        this.svc = svc;
        this.dbNames = dbNames;
    }

    public DbStatsSampler(String connectionId, MongoService svc) {
        this(connectionId, svc, () -> svc.listDatabaseNames().stream()
                .filter(n -> !n.equals("admin") && !n.equals("local") && !n.equals("config"))
                .toList());
    }

    @Override public SamplerKind kind() { return SamplerKind.DB_STATS; }
    @Override public String connectionId() { return connectionId; }

    @Override
    public List<MetricSample> sample(Instant now) {
        long ts = now.toEpochMilli();
        List<MetricSample> out = new ArrayList<>();
        for (String db : dbNames.get()) {
            Document stats;
            try { stats = svc.dbStats(db); }
            catch (Throwable t) { continue; }
            LabelSet lbl = LabelSet.of("db", db);
            emit(out, MetricId.DBSTAT_1, lbl, ts, DocUtil.longVal(stats, "collections", 0));
            emit(out, MetricId.DBSTAT_2, lbl, ts, DocUtil.longVal(stats, "views", 0));
            emit(out, MetricId.DBSTAT_3, lbl, ts, DocUtil.longVal(stats, "objects", 0));
            long dataSize    = DocUtil.longVal(stats, "dataSize", 0);
            long storageSize = DocUtil.longVal(stats, "storageSize", 0);
            long indexSize   = DocUtil.longVal(stats, "indexSize", 0);
            long totalSize   = DocUtil.longVal(stats, "totalSize", dataSize + indexSize);
            emit(out, MetricId.DBSTAT_4, lbl, ts, dataSize);
            emit(out, MetricId.DBSTAT_5, lbl, ts, storageSize);
            emit(out, MetricId.DBSTAT_6, lbl, ts, indexSize);
            emit(out, MetricId.DBSTAT_7, lbl, ts, totalSize);
            emit(out, MetricId.DBSTAT_8, lbl, ts, DocUtil.longVal(stats, "freeStorageSize", 0));
            if (storageSize > 0) {
                emit(out, MetricId.DBSTAT_9, lbl, ts, (double) (storageSize - dataSize) / storageSize);
            }
            emit(out, MetricId.DBSTAT_10, lbl, ts, DocUtil.doubleVal(stats, "avgObjSize", 0));
            emit(out, MetricId.DBSTAT_11, lbl, ts, DocUtil.longVal(stats, "indexes", 0));
        }
        return out;
    }

    private void emit(List<MetricSample> out, MetricId id, LabelSet l, long ts, double v) {
        out.add(new MetricSample(connectionId, id, l, ts, v));
    }
}
