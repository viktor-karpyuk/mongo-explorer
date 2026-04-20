package com.kubrik.mex.cluster.service;

import com.kubrik.mex.cluster.ops.LockInfo;
import com.kubrik.mex.core.MongoService;
import com.mongodb.MongoCommandException;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * v2.4 LOCK-1..4 — one-shot {@code lockInfo} probe. Returns
 * {@link LockInfo#unsupported()} on 3.4 and older (where the command doesn't
 * exist) or on permission denial so the UI can render the "not supported"
 * card without surfacing an error banner.
 */
public final class LockInfoService {

    private static final int MAX_TIME_MS = 3_000;

    private LockInfoService() {}

    public static LockInfo sample(MongoService svc) {
        if (svc == null) return LockInfo.unsupported();
        Document raw;
        try {
            Document cmd = new Document("lockInfo", 1).append("maxTimeMS", MAX_TIME_MS);
            raw = svc.database("admin").runCommand(cmd);
        } catch (MongoCommandException mce) {
            return LockInfo.unsupported();
        } catch (Exception e) {
            return LockInfo.unsupported();
        }
        return parse(raw);
    }

    @SuppressWarnings("unchecked")
    public static LockInfo parse(Document raw) {
        if (raw == null) return LockInfo.unsupported();
        Object lockInfoField = raw.get("lockInfo");
        if (!(lockInfoField instanceof List<?> list)) return LockInfo.unsupported();

        List<LockInfo.Entry> entries = new ArrayList<>();
        List<LockInfo.TopHolder> holders = new ArrayList<>();
        for (Object o : list) {
            if (!(o instanceof Document d)) continue;
            String resource = String.valueOf(d.get("resourceId"));
            if (d.get("resource") instanceof Document r) {
                String type = r.getString("type");
                String ns = r.getString("ns");
                resource = (type != null ? type : "") + (ns != null && !ns.isBlank() ? ":" + ns : "");
                if (resource.isBlank()) resource = String.valueOf(d.get("resourceId"));
            }
            int holdersCount = 0;
            int waitersCount = 0;
            long maxHold = 0;
            String mode = "";
            if (d.get("granted") instanceof List<?> g) {
                holdersCount = g.size();
                for (Object x : g) {
                    if (x instanceof Document gx) {
                        Object opid = gx.get("opid");
                        long opidL = opid instanceof Number n ? n.longValue() : 0L;
                        Object held = gx.get("microsHeld");
                        long heldMs = held instanceof Number n ? n.longValue() / 1000 : 0L;
                        if (heldMs > maxHold) maxHold = heldMs;
                        if (mode.isBlank() && gx.get("mode") instanceof String s) mode = s;
                        if (opidL != 0) holders.add(new LockInfo.TopHolder(opidL, resource, heldMs));
                    }
                }
            }
            if (d.get("pending") instanceof List<?> p) waitersCount = p.size();
            entries.add(new LockInfo.Entry(resource, holdersCount, waitersCount, maxHold, mode));
        }
        holders.sort(Comparator.comparingLong(LockInfo.TopHolder::heldMs).reversed());
        if (holders.size() > 5) holders = holders.subList(0, 5);
        return new LockInfo(true, entries, holders);
    }
}
