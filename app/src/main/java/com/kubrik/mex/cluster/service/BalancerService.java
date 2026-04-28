package com.kubrik.mex.cluster.service;

import com.kubrik.mex.cluster.ops.BalancerStatus;
import com.kubrik.mex.core.MongoService;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 * v2.4 SHARD-5..9 — one-shot probe of balancer state + activity metrics. Reads
 * {@code balancerStatus} (4.4+) for mode + round info, {@code config.settings}
 * for the {@code activeWindow} if set, {@code config.migrations} for in-flight
 * migrations, and {@code config.changelog} for chunks moved in the last 24 h.
 * Fails soft — any step that errors contributes its zero / null default so the
 * UI still renders the sections that succeeded.
 */
public final class BalancerService {

    private static final int MAX_TIME_MS = 3_000;

    private BalancerService() {}

    public static BalancerStatus sample(MongoService svc) {
        if (svc == null) return BalancerStatus.unsupported("not_connected");
        String mode = "unknown";
        boolean inRound = false;
        long rounds = 0;
        try {
            Document status = svc.database("admin").runCommand(
                    new Document("balancerStatus", 1).append("maxTimeMS", MAX_TIME_MS));
            mode = status.getString("mode");
            if (mode == null) {
                // Pre-4.4 servers used {ok: 1, inBalancerRound: ...} without mode.
                mode = Boolean.TRUE.equals(status.getBoolean("enabled")) ? "full" : "off";
            }
            inRound = Boolean.TRUE.equals(status.getBoolean("inBalancerRound"));
            Object nr = status.get("numBalancerRounds");
            if (nr instanceof Number n) rounds = n.longValue();
        } catch (Exception e) {
            return BalancerStatus.unsupported(e.getMessage());
        }

        String windowStart = null, windowStop = null;
        try {
            MongoDatabase config = svc.database("config");
            Document settings = config.getCollection("settings").find(
                    new Document("_id", "balancer")).first();
            if (settings != null && settings.get("activeWindow") instanceof Document w) {
                windowStart = w.getString("start");
                windowStop  = w.getString("stop");
            }
        } catch (Exception ignored) {}

        int active = 0;
        try {
            MongoCollection<Document> mig = svc.database("config").getCollection("migrations");
            active = (int) mig.countDocuments();
        } catch (Exception ignored) {}

        long moved24h = 0L;
        try {
            long since = System.currentTimeMillis() - 24L * 3_600_000L;
            MongoCollection<Document> changelog = svc.database("config").getCollection("changelog");
            moved24h = changelog.countDocuments(new Document("what",
                    new Document("$in", java.util.List.of("moveChunk.commit", "moveChunk.from")))
                    .append("time", new Document("$gte", new java.util.Date(since))));
        } catch (Exception ignored) {}

        return new BalancerStatus(true, mode, inRound, rounds, windowStart, windowStop,
                active, moved24h, null);
    }
}
