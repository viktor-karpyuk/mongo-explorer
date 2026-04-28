package com.kubrik.mex.maint.index;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.List;
import java.util.Optional;

/**
 * v2.7 IDX-BLD-5 — Tails {@code $currentOp} for the in-flight
 * {@code IXBUILD} entry belonging to a specific member / collection,
 * so the wizard can render an ETA.
 */
public final class BuildProgressTailer {

    public record Progress(
            long opId,
            long totalDocs,
            long processedDocs,
            String desc,
            long millisRunning
    ) {
        /** 0.0–1.0, or {@link Double#NaN} if total is unknown. */
        public double fraction() {
            if (totalDocs <= 0) return Double.NaN;
            return Math.min(1.0, processedDocs / (double) totalDocs);
        }
    }

    public Optional<Progress> probe(MongoClient client, String ns) {
        MongoDatabase admin = client.getDatabase("admin");
        Document currentOp = admin.runCommand(new Document("currentOp", 1)
                .append("$all", true));
        @SuppressWarnings("unchecked")
        List<Document> inProg = (List<Document>) currentOp.get("inprog", List.class);
        if (inProg == null) return Optional.empty();
        for (Document op : inProg) {
            String desc = op.getString("desc");
            if (desc == null || !desc.contains("IXBUILD")) continue;
            String opNs = op.getString("ns");
            if (opNs != null && !opNs.equals(ns)) continue;

            Document progress = op.get("progress", Document.class);
            long total = progress == null ? 0L
                    : progress.get("total") instanceof Number n ? n.longValue() : 0L;
            long done = progress == null ? 0L
                    : progress.get("done") instanceof Number n ? n.longValue() : 0L;
            Number opId = (Number) op.get("opid");
            Number ms = (Number) op.get("microsecs_running");
            return Optional.of(new Progress(
                    opId == null ? -1L : opId.longValue(),
                    total, done, desc,
                    ms == null ? 0L : ms.longValue() / 1000));
        }
        return Optional.empty();
    }
}
