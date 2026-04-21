package com.kubrik.mex.monitoring.sampler;

import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.monitoring.model.LabelSet;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.util.DocUtil;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** SHARD-* — only meaningful on a mongos router. */
public final class ShardingSampler implements Sampler {

    private final String connectionId;
    private final MongoService svc;

    public ShardingSampler(String connectionId, MongoService svc) {
        this.connectionId = connectionId;
        this.svc = svc;
    }

    @Override public SamplerKind kind() { return SamplerKind.SHARDING; }
    @Override public String connectionId() { return connectionId; }

    @Override
    public List<MetricSample> sample(Instant now) {
        long ts = now.toEpochMilli();
        List<MetricSample> out = new ArrayList<>();
        try {
            MongoDatabase config = svc.database("config");

            // Balancer state
            Document bsettings = config.getCollection("settings")
                    .find(Filters.eq("_id", "balancer")).first();
            boolean stopped = bsettings != null
                    && Boolean.TRUE.equals(bsettings.getBoolean("stopped"));
            emit(out, MetricId.SHARD_BAL_1, LabelSet.EMPTY, ts, stopped ? 0 : 1);

            // Chunks per shard
            MongoCollection<Document> chunks = config.getCollection("chunks");
            Map<String, Long> perShard = new HashMap<>();
            chunks.aggregate(List.of(
                    new Document("$group", new Document("_id", "$shard")
                            .append("count", new Document("$sum", 1)))
            )).forEach(d -> perShard.put(d.getString("_id"), DocUtil.longVal(d, "count", 0)));

            long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
            for (var e : perShard.entrySet()) {
                emit(out, MetricId.SHARD_CHK_1, LabelSet.of("shard", e.getKey()), ts, e.getValue());
                if (e.getValue() < min) min = e.getValue();
                if (e.getValue() > max) max = e.getValue();
            }
            if (!perShard.isEmpty()) {
                emit(out, MetricId.SHARD_CHK_2, LabelSet.EMPTY, ts, max - min);
            }
            emit(out, MetricId.SHARD_CHK_3, LabelSet.EMPTY, ts,
                    chunks.countDocuments(Filters.eq("jumbo", true)));

            // Migrations in last 24 h
            MongoCollection<Document> changelog = config.getCollection("changelog");
            long nowMs = now.toEpochMilli();
            long dayAgo = nowMs - 24L * 3600 * 1000;
            long success = changelog.countDocuments(Filters.and(
                    Filters.eq("what", "moveChunk.commit"),
                    Filters.gt("time", new Date(dayAgo))));
            long failed = changelog.countDocuments(Filters.and(
                    Filters.eq("what", "moveChunk.error"),
                    Filters.gt("time", new Date(dayAgo))));
            emit(out, MetricId.SHARD_MIG_1, LabelSet.EMPTY, ts, success);
            emit(out, MetricId.SHARD_MIG_2, LabelSet.EMPTY, ts, failed);
            Document lastMig = changelog.find(Filters.eq("what", "moveChunk.commit"))
                    .sort(Sorts.descending("time")).limit(1).first();
            if (lastMig != null && lastMig.get("time") instanceof Date d) {
                emit(out, MetricId.SHARD_MIG_3, LabelSet.EMPTY, ts, d.getTime());
            }

            // Mongos
            long mgsCount = config.getCollection("mongos")
                    .countDocuments(Filters.gt("ping", new Date(nowMs - 60_000)));
            emit(out, MetricId.SHARD_MGS_1, LabelSet.EMPTY, ts, mgsCount);
        } catch (Throwable ignored) {
            // Not a sharded cluster — silently produce nothing.
        }
        return out;
    }

    private void emit(List<MetricSample> out, MetricId id, LabelSet l, long ts, double v) {
        out.add(new MetricSample(connectionId, id, l, ts, v));
    }
}
