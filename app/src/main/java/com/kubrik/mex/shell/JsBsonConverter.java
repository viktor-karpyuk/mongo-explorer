package com.kubrik.mex.shell;

import org.bson.BsonDocument;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.graalvm.polyglot.Value;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Bridges between Graal {@link Value} (the JS side) and BSON
 * ({@link Document}, {@link java.util.List}, primitives, and the BSON
 * literal types). Used both ways: arguments come in as Value, results
 * go back out as Java/BSON which Graal exposes to JS automatically.
 */
final class JsBsonConverter {

    private JsBsonConverter() {}

    /** JS object/array/scalar → BSON-compatible Java value. */
    static Object toJava(Value v) {
        if (v == null || v.isNull()) return null;
        // Pass-through host objects (ObjectId, Date, Decimal128, UUID, Binary, etc.)
        if (v.isHostObject()) return v.asHostObject();
        if (v.isProxyObject()) {
            // Proxies created by our own shell — unwrap if they carry a marker.
            // (Currently our shell proxies are not data — they are call sites —
            // so this branch is mostly defensive.)
            return v.toString();
        }
        if (v.isBoolean()) return v.asBoolean();
        if (v.isString())  return v.asString();
        if (v.isNumber()) {
            if (v.fitsInInt())  return v.asInt();
            if (v.fitsInLong()) return v.asLong();
            return v.asDouble();
        }
        // JS Date — Graal exposes this as Instant (under the JS bindings)
        // but only when host-access allows; safer to detect via getMember
        // on "getTime".
        if (v.hasMember("getTime") && v.getMember("getTime").canExecute()) {
            try {
                long ms = v.getMember("getTime").execute().asLong();
                return new Date(ms);
            } catch (Exception ignored) { /* fall through */ }
        }
        if (v.hasArrayElements()) {
            long n = v.getArraySize();
            List<Object> out = new ArrayList<>((int) Math.min(n, Integer.MAX_VALUE));
            for (long i = 0; i < n; i++) out.add(toJava(v.getArrayElement(i)));
            return out;
        }
        if (v.hasMembers()) {
            Document d = new Document();
            for (String k : v.getMemberKeys()) {
                d.put(k, toJava(v.getMember(k)));
            }
            return d;
        }
        // Fallback — toString. Avoids returning the Value itself which the
        // driver will reject downstream.
        return v.toString();
    }

    /** Convenience: coerce a JS value that should be a BSON document. */
    static Document toDocument(Value v) {
        if (v == null || v.isNull()) return new Document();
        Object o = toJava(v);
        if (o instanceof Document d) return d;
        if (o instanceof Map<?,?> m) {
            Document d = new Document();
            for (Map.Entry<?,?> e : m.entrySet()) d.put(String.valueOf(e.getKey()), e.getValue());
            return d;
        }
        throw new IllegalArgumentException("expected an object document, got: " + (o == null ? "null" : o.getClass().getSimpleName()));
    }

    /** Coerce a JS array of objects → list of {@link Document}. */
    @SuppressWarnings("unchecked")
    static List<Document> toDocumentList(Value v) {
        if (v == null || v.isNull()) return List.of();
        if (!v.hasArrayElements()) {
            return List.of(toDocument(v));
        }
        long n = v.getArraySize();
        List<Document> out = new ArrayList<>((int) Math.min(n, Integer.MAX_VALUE));
        for (long i = 0; i < n; i++) out.add(toDocument(v.getArrayElement(i)));
        return out;
    }

    /**
     * Render an arbitrary BSON / Java value as a mongosh-flavoured
     * pseudo-JSON string. Not strict JSON — preserves {@code
     * ObjectId("…")}, {@code ISODate("…")}, {@code NumberLong("…")}
     * and friends so the output is round-trip-pasteable into the
     * shell.
     */
    static String prettyPrint(Object value) {
        StringBuilder sb = new StringBuilder();
        writeValue(sb, value, 0);
        return sb.toString();
    }

    private static void writeValue(StringBuilder sb, Object v, int indent) {
        if (v == null) { sb.append("null"); return; }
        if (v instanceof Document d)              { writeMap(sb, d, indent); return; }
        if (v instanceof BsonDocument bd)         { sb.append(bd.toJson()); return; }
        if (v instanceof Map<?,?> m)              { writeMap(sb, m, indent); return; }
        if (v instanceof Iterable<?> it)          { writeArray(sb, it, indent); return; }
        if (v instanceof ObjectId oid)            { sb.append("ObjectId(\"").append(oid.toHexString()).append("\")"); return; }
        if (v instanceof Date date)               { sb.append("ISODate(\"").append(java.time.Instant.ofEpochMilli(date.getTime())).append("\")"); return; }
        if (v instanceof Decimal128 dec)          { sb.append("NumberDecimal(\"").append(dec).append("\")"); return; }
        if (v instanceof Long lv)                 { sb.append("NumberLong(\"").append(lv).append("\")"); return; }
        if (v instanceof UUID uuid)               { sb.append("UUID(\"").append(uuid).append("\")"); return; }
        if (v instanceof Binary bin)              {
            sb.append("BinData(").append(bin.getType()).append(", \"")
              .append(java.util.Base64.getEncoder().encodeToString(bin.getData())).append("\")"); return;
        }
        if (v instanceof BsonTimestamp ts)        { sb.append("Timestamp(").append(ts.getTime()).append(", ").append(ts.getInc()).append(")"); return; }
        if (v instanceof BsonRegularExpression r) { sb.append("RegExp(\"").append(escape(r.getPattern())).append("\", \"").append(r.getOptions()).append("\")"); return; }
        if (v instanceof MinKey)                  { sb.append("MinKey()"); return; }
        if (v instanceof MaxKey)                  { sb.append("MaxKey()"); return; }
        if (v instanceof CharSequence s)          { sb.append('"').append(escape(s.toString())).append('"'); return; }
        if (v instanceof Boolean || v instanceof Number) { sb.append(v); return; }
        sb.append('"').append(escape(String.valueOf(v))).append('"');
    }

    private static void writeMap(StringBuilder sb, Map<?,?> map, int indent) {
        if (map.isEmpty()) { sb.append("{}"); return; }
        sb.append("{\n");
        int i = 0, n = map.size();
        for (Map.Entry<?,?> e : map.entrySet()) {
            indent(sb, indent + 1);
            sb.append('"').append(escape(String.valueOf(e.getKey()))).append("\": ");
            writeValue(sb, e.getValue(), indent + 1);
            if (++i < n) sb.append(',');
            sb.append('\n');
        }
        indent(sb, indent);
        sb.append('}');
    }

    private static void writeArray(StringBuilder sb, Iterable<?> it, int indent) {
        List<Object> items = new ArrayList<>();
        it.forEach(items::add);
        if (items.isEmpty()) { sb.append("[]"); return; }
        sb.append("[\n");
        for (int i = 0; i < items.size(); i++) {
            indent(sb, indent + 1);
            writeValue(sb, items.get(i), indent + 1);
            if (i < items.size() - 1) sb.append(',');
            sb.append('\n');
        }
        indent(sb, indent);
        sb.append(']');
    }

    private static void indent(StringBuilder sb, int depth) {
        for (int i = 0; i < depth; i++) sb.append("  ");
    }

    private static String escape(String s) {
        StringBuilder out = new StringBuilder(s.length() + 4);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\': out.append("\\\\"); break;
                case '"':  out.append("\\\""); break;
                case '\n': out.append("\\n");  break;
                case '\r': out.append("\\r");  break;
                case '\t': out.append("\\t");  break;
                default:
                    if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
                    else out.append(c);
            }
        }
        return out.toString();
    }
}
