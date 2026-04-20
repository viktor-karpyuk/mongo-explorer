package com.kubrik.mex.cluster.util;

import com.kubrik.mex.cluster.model.Member;

import java.util.Map;
import java.util.TreeMap;

/**
 * Minimal, deterministic JSON writer for topology snapshots. Keys are emitted
 * in sorted order, whitespace is stripped, numbers are normalised, and string
 * values are escaped per RFC 8259. This is intentionally *not* a general-purpose
 * JSON library — it produces exactly the canonical form v2.4 needs to derive a
 * stable {@code sha256} for {@code topology_snapshots}.
 */
public final class CanonicalJson {

    private CanonicalJson() {}

    public static ObjectWriter object() { return new ObjectWriter(new StringBuilder()); }

    /** Writer for a canonical JSON object. Keys are buffered and emitted sorted. */
    public static final class ObjectWriter {
        private final StringBuilder parent;
        private final TreeMap<String, String> entries = new TreeMap<>();
        private boolean closed = false;

        ObjectWriter(StringBuilder parent) { this.parent = parent; }

        public void putString(String key, String value) {
            entries.put(key, value == null ? "null" : quote(value));
        }

        public void putLong(String key, long value) {
            entries.put(key, Long.toString(value));
        }

        public void putNullableLong(String key, Long value) {
            entries.put(key, value == null ? "null" : Long.toString(value));
        }

        public void putNullableInt(String key, Integer value) {
            entries.put(key, value == null ? "null" : Integer.toString(value));
        }

        public void putBool(String key, Boolean value) {
            entries.put(key, value == null ? "null" : (value ? "true" : "false"));
        }

        public ObjectWriter putObject(String key) {
            StringBuilder child = new StringBuilder();
            ObjectWriter w = new ObjectWriter(child);
            entries.put(key, "__obj__" + System.identityHashCode(w));
            // Object contents are rendered when toJson() is called on the child; we
            // stash a sentinel here, then resolve it by replacing with the child's
            // final JSON at flush time.
            pending.put("__obj__" + System.identityHashCode(w), child);
            childClosers.put("__obj__" + System.identityHashCode(w), w);
            return w;
        }

        public ArrayWriter putArray(String key) {
            StringBuilder child = new StringBuilder();
            ArrayWriter w = new ArrayWriter(child);
            entries.put(key, "__arr__" + System.identityHashCode(w));
            pending.put("__arr__" + System.identityHashCode(w), child);
            childArrClosers.put("__arr__" + System.identityHashCode(w), w);
            return w;
        }

        public void close() { closed = true; }

        /** Render the object plus its children into canonical JSON. */
        public String toJson() {
            close();
            // Resolve any nested sentinels.
            StringBuilder out = new StringBuilder();
            out.append('{');
            boolean first = true;
            for (Map.Entry<String, String> e : entries.entrySet()) {
                if (!first) out.append(',');
                first = false;
                out.append(quote(e.getKey())).append(':');
                String v = e.getValue();
                if (v.startsWith("__obj__")) {
                    ObjectWriter child = childClosers.get(v);
                    out.append(child.toJson());
                } else if (v.startsWith("__arr__")) {
                    ArrayWriter child = childArrClosers.get(v);
                    out.append(child.toJson());
                } else {
                    out.append(v);
                }
            }
            out.append('}');
            if (parent != null) parent.append(out);
            return out.toString();
        }

        // Per-object scratchpads for nested children.
        private final java.util.Map<String, StringBuilder> pending = new java.util.HashMap<>();
        private final java.util.Map<String, ObjectWriter> childClosers = new java.util.HashMap<>();
        private final java.util.Map<String, ArrayWriter> childArrClosers = new java.util.HashMap<>();
    }

    public static final class ArrayWriter {
        private final StringBuilder parent;
        private final java.util.List<String> items = new java.util.ArrayList<>();
        private final java.util.List<ObjectWriter> nestedObjects = new java.util.ArrayList<>();
        private boolean closed = false;

        ArrayWriter(StringBuilder parent) { this.parent = parent; }

        public ObjectWriter addObject() {
            ObjectWriter w = new ObjectWriter(new StringBuilder());
            nestedObjects.add(w);
            items.add("__idx__" + (nestedObjects.size() - 1));
            return w;
        }

        public void addString(String s) {
            items.add(s == null ? "null" : quote(s));
        }

        public void close() { closed = true; }

        public String toJson() {
            close();
            StringBuilder out = new StringBuilder();
            out.append('[');
            boolean first = true;
            for (String v : items) {
                if (!first) out.append(',');
                first = false;
                if (v.startsWith("__idx__")) {
                    int idx = Integer.parseInt(v.substring(7));
                    out.append(nestedObjects.get(idx).toJson());
                } else {
                    out.append(v);
                }
            }
            out.append(']');
            if (parent != null) parent.append(out);
            return out.toString();
        }
    }

    /** RFC 8259 string quoting. Visible across the cluster package for reuse. */
    public static String quote(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2);
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"'  -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        sb.append('"');
        return sb.toString();
    }

    /** Render a {@link Member} into a pre-created object writer. Delegated here
     *  so the record class stays free of JSON concerns. */
    public static void writeMember(ObjectWriter w, Member m) {
        w.putString("host", m.host());
        w.putString("state", m.state().name());
        w.putNullableInt("priority", m.priority());
        w.putNullableInt("votes", m.votes());
        w.putBool("hidden", m.hidden());
        w.putBool("arbiterOnly", m.arbiterOnly());
        writeTags(w.putObject("tags"), m.tags());
        w.putNullableLong("optimeMs", m.optimeMs());
        w.putNullableLong("lagMs", m.lagMs());
        w.putNullableLong("pingMs", m.pingMs());
        w.putNullableLong("uptimeSecs", m.uptimeSecs());
        w.putString("syncSourceHost", m.syncSourceHost());
        w.putNullableInt("configVersion", m.configVersion());
        w.close();
    }

    public static void writeTags(ObjectWriter w, Map<String, String> tags) {
        if (tags != null) tags.forEach(w::putString);
        w.close();
    }
}
