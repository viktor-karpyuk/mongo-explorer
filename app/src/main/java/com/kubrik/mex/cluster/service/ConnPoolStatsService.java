package com.kubrik.mex.cluster.service;

import com.kubrik.mex.cluster.ops.ConnPoolStats;
import com.kubrik.mex.core.MongoService;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * v2.4 POOL-1..5 — one-shot probe of {@code connPoolStats}. The server returns
 * a {@code hosts} sub-document keyed by "host:port"; each entry carries
 * {@code inUse / available / created / refreshing} counters plus a
 * {@code waitQueueSize} that we flag amber in the UI when non-zero.
 */
public final class ConnPoolStatsService {

    private static final int MAX_TIME_MS = 3_000;

    private ConnPoolStatsService() {}

    public static ConnPoolStats sample(MongoService svc) {
        if (svc == null) return ConnPoolStats.empty();
        Document raw;
        try {
            Document cmd = new Document("connPoolStats", 1).append("maxTimeMS", MAX_TIME_MS);
            raw = svc.database("admin").runCommand(cmd);
        } catch (Exception e) {
            return ConnPoolStats.empty();
        }
        return parse(raw);
    }

    @SuppressWarnings("unchecked")
    public static ConnPoolStats parse(Document raw) {
        if (raw == null) return ConnPoolStats.empty();
        List<ConnPoolStats.Row> rows = new ArrayList<>();
        Object hostsField = raw.get("hosts");
        if (hostsField instanceof Document hosts) {
            for (java.util.Map.Entry<String, Object> e : hosts.entrySet()) {
                if (!(e.getValue() instanceof Document d)) continue;
                rows.add(rowFrom(e.getKey(), d));
            }
        }
        rows.sort(Comparator.comparing(ConnPoolStats.Row::host));
        int inUse = intValue(raw.get("totalInUse"));
        int avail = intValue(raw.get("totalAvailable"));
        int created = intValue(raw.get("totalCreated"));
        return new ConnPoolStats(rows, inUse, avail, created);
    }

    private static ConnPoolStats.Row rowFrom(String host, Document d) {
        int poolSize = intValue(d.get("poolSize"));
        int inUse    = intValue(d.get("inUse"));
        int avail    = intValue(d.get("available"));
        int created  = intValue(d.get("created"));
        int refreshing = intValue(d.get("refreshing"));
        int waitQueue = intValue(d.get("waitQueueSize"));
        long timeouts = longValue(d.get("timeouts"));
        Long refreshedMs = null;
        Object rf = d.get("lastRefreshedDate");
        if (rf instanceof java.util.Date date) refreshedMs = date.getTime();
        return new ConnPoolStats.Row(host, poolSize, inUse, avail, created,
                refreshing, waitQueue, timeouts, refreshedMs);
    }

    private static int intValue(Object o) {
        return o instanceof Number n ? n.intValue() : 0;
    }

    private static long longValue(Object o) {
        return o instanceof Number n ? n.longValue() : 0L;
    }
}
