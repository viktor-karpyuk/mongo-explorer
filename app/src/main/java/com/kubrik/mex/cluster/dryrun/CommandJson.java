package com.kubrik.mex.cluster.dryrun;

import com.kubrik.mex.cluster.util.CanonicalJson;

import java.util.Map;
import java.util.TreeMap;

/**
 * Canonical JSON for command-style BSON payloads: sorted keys, no whitespace,
 * integers without decimals. Used by {@link DryRunRenderer} so
 * {@link com.kubrik.mex.cluster.safety.DryRunResult#previewHash()} is stable.
 *
 * <p>This is a narrower helper than {@code CanonicalJson} (which is tuned for
 * the topology-snapshot record shape). {@code render} accepts any nested
 * {@code Map} / {@code List} tree of JSON primitives.</p>
 */
public final class CommandJson {

    private CommandJson() {}

    public static String render(Map<String, ?> tree) {
        StringBuilder sb = new StringBuilder();
        writeObject(sb, tree);
        return sb.toString();
    }

    private static void writeValue(StringBuilder sb, Object v) {
        if (v == null)                 sb.append("null");
        else if (v instanceof String s)  sb.append(CanonicalJson.quote(s));
        else if (v instanceof Boolean b) sb.append(b ? "true" : "false");
        else if (v instanceof Integer i) sb.append(i.intValue());
        else if (v instanceof Long l)    sb.append(l.longValue());
        else if (v instanceof Short s)   sb.append(s.intValue());
        else if (v instanceof Number n)  sb.append(formatNumber(n));
        else if (v instanceof Map<?, ?> m) writeObject(sb, m);
        else if (v instanceof Iterable<?> it) {
            sb.append('[');
            boolean first = true;
            for (Object el : it) {
                if (!first) sb.append(',');
                first = false;
                writeValue(sb, el);
            }
            sb.append(']');
        } else sb.append(CanonicalJson.quote(v.toString()));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void writeObject(StringBuilder sb, Map<?, ?> in) {
        TreeMap<String, Object> sorted = new TreeMap<>();
        for (Map.Entry e : in.entrySet()) sorted.put(String.valueOf(e.getKey()), e.getValue());
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String, Object> e : sorted.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append(CanonicalJson.quote(e.getKey())).append(':');
            writeValue(sb, e.getValue());
        }
        sb.append('}');
    }

    private static String formatNumber(Number n) {
        double d = n.doubleValue();
        if (d == Math.floor(d) && !Double.isInfinite(d)) {
            long asLong = (long) d;
            if ((double) asLong == d) return Long.toString(asLong);
        }
        String s = Double.toString(d);
        // Trim trailing zeros after the decimal point but keep at least one digit.
        if (s.contains(".") && !s.contains("e") && !s.contains("E")) {
            s = s.replaceAll("0+$", "");
            if (s.endsWith(".")) s = s + "0";
        }
        return s;
    }
}
