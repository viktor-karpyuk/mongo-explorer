package com.kubrik.mex.security.audit;

import org.bson.Document;
import org.bson.json.JsonParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * v2.6 Q2.6-C2 — parser for MongoDB's native audit-log line format. Each
 * line is a JSON object; we delegate the lexing to the BSON {@link
 * Document#parse} since it already handles the extended-JSON
 * {@code $date} / {@code $oid} wrappers MongoDB emits.
 *
 * <p>We keep the parse tolerant: any line that fails to parse returns
 * {@code null} so the tailer can log + skip without crashing the stream.
 * An audit log from a compromised process trying to break the parser
 * with malformed lines is a realistic concern, so the live-thread
 * tailer treats {@code null} as "advance and continue".</p>
 */
public final class AuditEventParser {

    private static final Logger log = LoggerFactory.getLogger(AuditEventParser.class);

    private AuditEventParser() {}

    public static AuditEvent parse(String line) {
        if (line == null || line.isBlank()) return null;
        Document d;
        try {
            d = Document.parse(line);
        } catch (JsonParseException e) {
            log.debug("audit line rejected: {}", e.getMessage());
            return null;
        }
        String atype = d.getString("atype");
        if (atype == null) return null;

        long tsMs = extractTs(d.get("ts"));
        String who = "";
        String whoDb = "";
        List<Document> users = d.getList("users", Document.class, List.of());
        if (!users.isEmpty()) {
            Document first = users.get(0);
            who = first.getString("user") == null ? "" : first.getString("user");
            whoDb = first.getString("db") == null ? "" : first.getString("db");
        }
        String fromHost = extractFromHost(d);
        int result = d.get("result") instanceof Number n ? n.intValue() : 0;

        Object paramNode = d.get("param");
        Map<String, Object> paramFlat = paramNode instanceof Document pd
                ? flatten(pd) : Map.of();

        return new AuditEvent(atype, tsMs, who, whoDb, fromHost, result,
                paramFlat, line);
    }

    /* ============================== helpers ============================== */

    private static long extractTs(Object ts) {
        if (ts instanceof java.util.Date dt) return dt.getTime();
        if (ts instanceof Number n) return n.longValue();
        if (ts instanceof Document td) {
            Object dateNode = td.get("$date");
            if (dateNode instanceof java.util.Date dt) return dt.getTime();
            if (dateNode instanceof Number n) return n.longValue();
            if (dateNode instanceof String s) {
                try { return Instant.parse(s).toEpochMilli(); }
                catch (Exception ignored) {}
            }
        }
        return 0L;
    }

    private static String extractFromHost(Document d) {
        Document remote = d.get("remote", Document.class);
        if (remote == null) return "";
        String ip = remote.getString("ip");
        Object port = remote.get("port");
        if (ip == null) return "";
        return port == null ? ip : ip + ":" + port;
    }

    private static Map<String, Object> flatten(Document d) {
        // Keep parameter trees one level flat so the FTS index can search
        // 'mechanism:SCRAM-SHA-256' without walking a nested BSON tree
        // at query time. Sub-documents are rendered as compact JSON
        // strings so their content still indexes.
        Map<String, Object> out = new LinkedHashMap<>();
        for (String k : d.keySet()) {
            Object v = d.get(k);
            if (v instanceof Document sub) out.put(k, sub.toJson());
            else if (v instanceof List<?> list) out.put(k, String.valueOf(list));
            else out.put(k, v);
        }
        return out;
    }
}
