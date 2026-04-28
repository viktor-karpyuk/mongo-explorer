package com.kubrik.mex.cluster.service;

import com.kubrik.mex.cluster.ops.TagRange;
import com.kubrik.mex.core.MongoService;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * v2.4 SHARD-14..16 — reads zone definitions and tag ranges from config.
 *
 * <p>{@link #tagRanges} queries {@code config.tags}; one document per range.
 * {@link #zonesPerShard} derives the shard ↔ zone mapping from
 * {@code config.shards.tags} (since 4.2+) so the UI can render both
 * directions of the mapping without another round-trip.</p>
 */
public final class ZonesService {

    private static final int MAX_TIME_MS = 3_000;
    private static final JsonWriterSettings RELAXED = JsonWriterSettings.builder().build();

    private ZonesService() {}

    public static List<TagRange> tagRanges(MongoService svc) {
        if (svc == null) return List.of();
        try {
            MongoCollection<Document> tags = svc.database("config").getCollection("tags");
            List<TagRange> out = new ArrayList<>();
            for (Document d : tags.find()) {
                String ns = d.getString("ns");
                String tag = d.getString("tag");
                Document min = asDoc(d.get("min"));
                Document max = asDoc(d.get("max"));
                out.add(new TagRange(
                        ns == null ? "" : ns,
                        tag == null ? "" : tag,
                        min.toJson(RELAXED),
                        max.toJson(RELAXED),
                        min, max));
            }
            return out;
        } catch (Exception e) {
            return List.of();
        }
    }

    /** Shard name → list of zones attached to that shard. Sorted for stable UI. */
    @SuppressWarnings("unchecked")
    public static Map<String, List<String>> zonesPerShard(MongoService svc) {
        if (svc == null) return Map.of();
        try {
            MongoCollection<Document> shards = svc.database("config").getCollection("shards");
            Map<String, List<String>> out = new LinkedHashMap<>();
            for (Document d : shards.find()) {
                String id = d.getString("_id");
                Object raw = d.get("tags");
                List<String> zones = new ArrayList<>();
                if (raw instanceof List<?> list) {
                    for (Object o : list) if (o instanceof String s) zones.add(s);
                }
                zones.sort(String::compareTo);
                if (id != null) out.put(id, zones);
            }
            return out;
        } catch (Exception e) {
            return Map.of();
        }
    }

    private static Document asDoc(Object o) {
        return o instanceof Document d ? d : new Document();
    }
}
