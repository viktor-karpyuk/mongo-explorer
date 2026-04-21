package com.kubrik.mex.monitoring.util;

import org.bson.Document;
import org.bson.types.Binary;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Per QPERF-3 / BR-9 — replaces string and binary literals with {@code "?"} inside
 * {@code filter}, {@code update}, {@code pipeline}, {@code query}, {@code args} keys.
 * Numeric, date, ObjectId, boolean values are preserved: they're harmless and
 * diagnostic. {@code queryHash} / {@code planCacheKey} are preserved verbatim.
 */
public final class Redactor {

    private static final Set<String> REDACT_ROOTS = Set.of(
            "filter", "update", "pipeline", "query", "args",
            "q", "u", "updates", "documents"
    );

    private Redactor() {}

    public static Document redact(Document cmd) {
        Document out = new Document();
        for (Map.Entry<String, Object> e : cmd.entrySet()) {
            String k = e.getKey();
            Object v = e.getValue();
            if (REDACT_ROOTS.contains(k)) {
                out.put(k, redactValue(v));
            } else {
                out.put(k, v);
            }
        }
        return out;
    }

    private static Object redactValue(Object v) {
        return switch (v) {
            case null -> null;
            case String s -> "?";
            case Binary b -> "?";
            case Document d -> {
                Document r = new Document();
                for (Map.Entry<String, Object> e : d.entrySet()) {
                    r.put(e.getKey(), redactValue(e.getValue()));
                }
                yield r;
            }
            case Map<?, ?> m -> {
                Map<Object, Object> r = new LinkedHashMap<>();
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    r.put(e.getKey(), redactValue(e.getValue()));
                }
                yield r;
            }
            case List<?> l -> {
                List<Object> r = new ArrayList<>(l.size());
                for (Object o : l) r.add(redactValue(o));
                yield r;
            }
            default -> v; // Number / Boolean / Date / ObjectId → keep
        };
    }
}
