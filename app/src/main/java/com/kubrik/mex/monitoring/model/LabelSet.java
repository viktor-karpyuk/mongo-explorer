package com.kubrik.mex.monitoring.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Immutable, sorted label map attached to every {@link MetricSample}. The compact
 * JSON form ({@link #toJson()}) is what the raw-sample PK indexes, so equality and
 * ordering must be stable.
 *
 * <p>BR-8: labels may only contain topology identifiers — never document content,
 * filter values, or credentials. Any other key throws {@link IllegalArgumentException}.
 */
public record LabelSet(Map<String, String> labels) {

    /** Keys permitted on a label set per BR-8. */
    public static final Set<String> ALLOWED_KEYS =
            Set.of("db", "coll", "shard", "member", "index", "host", "op_type");

    public static final LabelSet EMPTY = new LabelSet(Collections.emptyMap());

    public LabelSet {
        if (labels == null) throw new NullPointerException("labels");
        for (Map.Entry<String, String> e : labels.entrySet()) {
            if (!ALLOWED_KEYS.contains(e.getKey())) {
                throw new IllegalArgumentException("label key not allowed: " + e.getKey());
            }
            if (e.getValue() == null) {
                throw new IllegalArgumentException("null value for label " + e.getKey());
            }
        }
        labels = Collections.unmodifiableMap(new TreeMap<>(labels));
    }

    public static LabelSet of(String... kv) {
        if ((kv.length & 1) != 0) {
            throw new IllegalArgumentException("LabelSet.of requires an even number of args");
        }
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put(kv[i], kv[i + 1]);
        return new LabelSet(m);
    }

    /**
     * Canonical JSON form: sorted keys, no whitespace, double-quoted strings. String
     * values escape {@code \} and {@code "} only — label values are controlled by us
     * (topology identifiers) so a full JSON escaper is overkill.
     */
    public String toJson() {
        if (labels.isEmpty()) return "{}";
        StringBuilder sb = new StringBuilder(labels.size() * 24);
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String, String> e : labels.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append('"').append(e.getKey()).append("\":\"");
            appendEscaped(sb, e.getValue());
            sb.append('"');
        }
        sb.append('}');
        return sb.toString();
    }

    private static void appendEscaped(StringBuilder sb, String v) {
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            if (c == '\\' || c == '"') {
                sb.append('\\');
            }
            sb.append(c);
        }
    }
}
