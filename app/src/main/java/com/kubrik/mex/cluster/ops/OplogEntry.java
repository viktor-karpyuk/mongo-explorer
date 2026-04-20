package com.kubrik.mex.cluster.ops;

import org.bson.BsonTimestamp;
import org.bson.Document;

/**
 * v2.4 OPLOG-5..8 — view-model row for the oplog tail. Carries the raw entry
 * for the JSON detail drawer + a short preview for the table column.
 */
public record OplogEntry(
        BsonTimestamp ts,
        long tsSec,
        String op,
        String ns,
        String preview,
        Document raw
) {
    public static OplogEntry fromRaw(Document d) {
        BsonTimestamp ts = null;
        Object rawTs = d.get("ts");
        if (rawTs instanceof BsonTimestamp bt) ts = bt;
        long sec = ts == null ? 0L : ts.getTime();
        String op = d.getString("op");
        String ns = d.getString("ns");
        String preview = buildPreview(d);
        return new OplogEntry(ts, sec, op == null ? "" : op, ns == null ? "" : ns,
                preview, d);
    }

    private static String buildPreview(Document d) {
        Object o = d.get("o");
        if (!(o instanceof Document doc)) return "";
        String s = doc.toJson();
        return s.length() > 160 ? s.substring(0, 157) + "…" : s;
    }

    public String opLabel() {
        return switch (op) {
            case "i" -> "insert";
            case "u" -> "update";
            case "d" -> "delete";
            case "c" -> "command";
            case "n" -> "noop";
            default  -> op;
        };
    }
}
