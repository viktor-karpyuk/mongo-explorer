package com.kubrik.mex.cluster.service;

import com.kubrik.mex.cluster.ops.ChunkSummary;
import com.kubrik.mex.core.MongoService;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * v2.4 SHARD-10..13 — aggregates {@code config.chunks} (or {@code config.collections}
 * + {@code config.chunks} post-5.0 where chunks key off {@code uuid}) into the
 * per-collection {@link ChunkSummary} rows the chunk distribution pane
 * renders. Jumbo counts come from the {@code jumbo: true} flag on each chunk.
 */
public final class ChunkService {

    private static final int MAX_TIME_MS = 5_000;

    private ChunkService() {}

    public static List<ChunkSummary> distribution(MongoService svc) {
        if (svc == null) return List.of();
        try {
            MongoCollection<Document> chunks = svc.database("config").getCollection("chunks");
            // First: per-(ns, shard) counts + jumbo tally. 5.0+ switched to `uuid`
            // instead of `ns`; we coalesce both via $ifNull so the pipeline works
            // across versions.
            AggregateIterable<Document> agg = chunks.aggregate(List.of(
                    new Document("$group", new Document("_id",
                            new Document("ns", new Document("$ifNull", List.of("$ns", "$uuid")))
                                    .append("shard", "$shard"))
                            .append("count", new Document("$sum", 1))
                            .append("jumbo", new Document("$sum",
                                    new Document("$cond", List.of("$jumbo", 1, 0))))),
                    new Document("$sort", new Document("_id.ns", 1))
            )).maxTime(MAX_TIME_MS, java.util.concurrent.TimeUnit.MILLISECONDS);

            Map<String, Map<String, Long>> perNsPerShard = new LinkedHashMap<>();
            Map<String, Long> jumboByNs = new LinkedHashMap<>();
            Map<String, Long> totalByNs = new LinkedHashMap<>();

            for (Document d : agg) {
                Document id = (Document) d.get("_id");
                String ns = String.valueOf(id.get("ns"));
                String shard = id.getString("shard");
                long count = longValue(d.get("count"));
                long jumbo = longValue(d.get("jumbo"));
                perNsPerShard.computeIfAbsent(ns, k -> new LinkedHashMap<>()).put(shard, count);
                jumboByNs.merge(ns, jumbo, Long::sum);
                totalByNs.merge(ns, count, Long::sum);
            }

            // Resolve uuid → ns via config.collections for 5.0+ chunks that keyed on uuid.
            Map<String, String> uuidToNs = resolveUuids(svc, perNsPerShard.keySet());

            List<ChunkSummary> out = new ArrayList<>();
            for (var e : perNsPerShard.entrySet()) {
                String ns = uuidToNs.getOrDefault(e.getKey(), e.getKey());
                out.add(new ChunkSummary(ns,
                        totalByNs.getOrDefault(e.getKey(), 0L),
                        jumboByNs.getOrDefault(e.getKey(), 0L),
                        e.getValue()));
            }
            return out;
        } catch (Exception e) {
            return List.of();
        }
    }

    public static List<String> listShards(MongoService svc) {
        if (svc == null) return List.of();
        try {
            Document reply = svc.database("admin").runCommand(
                    new Document("listShards", 1).append("maxTimeMS", MAX_TIME_MS));
            Object shards = reply.get("shards");
            if (!(shards instanceof List<?> list)) return List.of();
            List<String> out = new ArrayList<>();
            for (Object o : list) {
                if (o instanceof Document d) {
                    String id = d.getString("_id");
                    if (id != null) out.add(id);
                }
            }
            return out;
        } catch (Exception e) {
            return List.of();
        }
    }

    private static Map<String, String> resolveUuids(MongoService svc, java.util.Set<String> candidates) {
        Map<String, String> out = new LinkedHashMap<>();
        if (candidates.isEmpty()) return out;
        try {
            MongoCollection<Document> colls = svc.database("config").getCollection("collections");
            for (Document d : colls.find()) {
                Object uuid = d.get("uuid");
                String ns = d.getString("_id");
                if (uuid != null && ns != null && candidates.contains(uuid.toString())) {
                    out.put(uuid.toString(), ns);
                }
            }
        } catch (Exception ignored) {}
        return out;
    }

    private static long longValue(Object o) {
        return o instanceof Number n ? n.longValue() : 0L;
    }
}
