package com.kubrik.mex.monitoring.sampler;

import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.monitoring.model.LabelSet;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.util.DocUtil;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.BiFunction;

/**
 * IDX-USE-* via the {@code $indexStats} aggregation stage. IDX-USE-4 (candidate
 * unused) is evaluated against server uptime (≥ 7 days required) — without uptime
 * information we err on the side of "not a candidate".
 */
public final class IndexStatsSampler implements Sampler {

    private final String connectionId;
    private final MongoService svc;
    private final BiFunction<String, String, List<Document>> indexStatsFetcher;
    private final CounterTracker rates = new CounterTracker();
    private final Supplier<Long> uptimeSecSupplier;

    public IndexStatsSampler(String connectionId, MongoService svc, Supplier<Long> uptimeSecSupplier) {
        this.connectionId = connectionId;
        this.svc = svc;
        this.uptimeSecSupplier = uptimeSecSupplier;
        this.indexStatsFetcher = (db, coll) -> {
            MongoCollection<Document> c = svc.collection(db, coll);
            AggregateIterable<Document> it = c.aggregate(List.of(new Document("$indexStats", new Document())));
            List<Document> out = new ArrayList<>();
            for (Document d : it) out.add(d);
            return out;
        };
    }

    @Override public SamplerKind kind() { return SamplerKind.INDEX_STATS; }
    @Override public String connectionId() { return connectionId; }

    @Override
    public List<MetricSample> sample(Instant now) {
        long ts = now.toEpochMilli();
        List<MetricSample> out = new ArrayList<>();
        long uptimeSec = uptimeSecSupplier.get();
        boolean uptimeOk = uptimeSec >= 7 * 24 * 3600L;
        List<String> dbs = svc.listDatabaseNames().stream()
                .filter(n -> !n.equals("admin") && !n.equals("local") && !n.equals("config"))
                .toList();
        for (String db : dbs) {
            List<String> colls;
            try { colls = svc.listCollectionNames(db); }
            catch (Throwable t) { continue; }
            for (String coll : colls) {
                List<Document> stats;
                try { stats = indexStatsFetcher.apply(db, coll); }
                catch (Throwable t) { continue; }
                for (Document s : stats) {
                    String idxName = s.getString("name");
                    if (idxName == null) continue;
                    LabelSet lbl = LabelSet.of("db", db, "coll", coll, "index", idxName);
                    Document accesses = DocUtil.sub(s, "accesses");
                    long ops = DocUtil.longVal(accesses, "ops", 0);
                    emit(out, MetricId.IDX_USE_1, lbl, ts, ops);
                    Date since = (Date) accesses.get("since");
                    if (since != null) emit(out, MetricId.IDX_USE_2, lbl, ts, since.getTime());
                    rates.rate(connectionId, MetricId.IDX_USE_3, lbl, ts, ops)
                            .ifPresent(v -> emit(out, MetricId.IDX_USE_3, lbl, ts, v));
                    boolean candidate = ops == 0 && uptimeOk;
                    emit(out, MetricId.IDX_USE_4, lbl, ts, candidate ? 1 : 0);
                }
            }
        }
        return out;
    }

    private void emit(List<MetricSample> out, MetricId id, LabelSet l, long ts, double v) {
        out.add(new MetricSample(connectionId, id, l, ts, v));
    }

    @FunctionalInterface public interface Supplier<T> { T get(); }
}
