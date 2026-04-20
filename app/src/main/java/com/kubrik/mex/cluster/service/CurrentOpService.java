package com.kubrik.mex.cluster.service;

import com.kubrik.mex.cluster.ops.CurrentOpRow;
import com.kubrik.mex.core.MongoService;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * v2.4 OP-1..2 — one-shot {@code $currentOp} query returning full op rows for
 * the UI. Runs with {@code allUsers: true, idleSessions: false} (matches the
 * monitoring-side sampler's scope). {@code maxTimeMS} is capped at 3 s so a
 * busy cluster can't block the UI poll tick.
 */
public final class CurrentOpService {

    private static final int MAX_TIME_MS = 3_000;

    private CurrentOpService() {}

    public static List<CurrentOpRow> sample(MongoService svc) {
        if (svc == null) return List.of();
        MongoDatabase admin = svc.database("admin");
        Document stage = new Document("$currentOp",
                new Document("allUsers", true).append("idleSessions", false));
        AggregateIterable<Document> it = admin.aggregate(List.of(stage))
                .maxTime(MAX_TIME_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
        List<CurrentOpRow> out = new ArrayList<>();
        for (Document d : it) out.add(CurrentOpRow.fromRaw(d));
        return out;
    }
}
