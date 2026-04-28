package com.kubrik.mex.security.drift;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * v2.6 Q2.6-D1 — structural diff between two baseline payloads. Walks two
 * generic {@code Map<String, Object>} / {@code List<Object>} trees (the
 * shape {@link com.kubrik.mex.security.baseline.SecurityBaselineCaptureService}
 * emits) and yields {@link DriftFinding} rows keyed on a dot-path the
 * ack workflow can reference.
 *
 * <p>Design notes:
 * <ul>
 *   <li>The engine is <em>type-weak on purpose</em>. We compare the
 *       payload maps — not the strongly-typed records — so additions to
 *       the capture shape don't require a diff-engine change. The cost
 *       is we treat {@code null} and missing as equivalent, which is
 *       what the pane wants anyway.</li>
 *   <li>List diffs are positional. That's fine for most security
 *       subjects (role bindings + privileges are position-stable because
 *       the capture sorts actions + insertion-orders users / roles). The
 *       only genuinely-unordered field is the server's reply order for
 *       inheritedPrivileges; we don't re-sort here because the capture
 *       already does.</li>
 *   <li>Path segments on list entries use {@code [i]} so the ack pane
 *       can render them as plain addresses.</li>
 * </ul>
 */
public final class DriftDiffEngine {

    private DriftDiffEngine() {}

    /** Diff two baseline payload trees. Callers that only have the raw
     *  stored JSON should parse once with any Map-producing parser; the
     *  engine doesn't care about the origin. */
    public static List<DriftFinding> diff(Map<String, Object> baseline,
                                           Map<String, Object> current) {
        Map<String, Object> b = baseline == null ? Map.of() : baseline;
        Map<String, Object> c = current == null ? Map.of() : current;
        List<DriftFinding> out = new ArrayList<>();
        diffInto(b, c, "", sectionOf(""), out);
        return out;
    }

    /* ============================== walker ============================== */

    @SuppressWarnings("unchecked")
    private static void diffInto(Object a, Object b, String path, String section,
                                  List<DriftFinding> out) {
        if (isMap(a) && isMap(b)) {
            diffMap((Map<String, Object>) a, (Map<String, Object>) b, path, section, out);
            return;
        }
        if (a instanceof List<?> la && b instanceof List<?> lb) {
            diffList(la, lb, path, section, out);
            return;
        }
        // Scalar (or mixed type). Use Objects.equals so null == null maps
        // to "no finding" and 3 != "3" surfaces as a type-change.
        if (!equalScalars(a, b)) {
            out.add(new DriftFinding(path,
                    a == null ? DriftFinding.Kind.ADDED
                            : b == null ? DriftFinding.Kind.REMOVED
                                    : DriftFinding.Kind.CHANGED,
                    toRendered(a), toRendered(b), section));
        }
    }

    private static void diffMap(Map<String, Object> a, Map<String, Object> b,
                                 String path, String section, List<DriftFinding> out) {
        TreeSet<String> keys = new TreeSet<>(a.keySet());
        keys.addAll(b.keySet());
        for (String key : keys) {
            Object av = a.get(key);
            Object bv = b.get(key);
            String childPath = path.isEmpty() ? key : path + "." + key;
            String childSection = path.isEmpty() ? sectionOf(key) : section;
            if (!a.containsKey(key)) {
                out.add(new DriftFinding(childPath, DriftFinding.Kind.ADDED,
                        null, toRendered(bv), childSection));
            } else if (!b.containsKey(key)) {
                out.add(new DriftFinding(childPath, DriftFinding.Kind.REMOVED,
                        toRendered(av), null, childSection));
            } else {
                diffInto(av, bv, childPath, childSection, out);
            }
        }
    }

    private static void diffList(List<?> a, List<?> b, String path,
                                  String section, List<DriftFinding> out) {
        int n = Math.max(a.size(), b.size());
        for (int i = 0; i < n; i++) {
            Object av = i < a.size() ? a.get(i) : null;
            Object bv = i < b.size() ? b.get(i) : null;
            String childPath = path + "[" + i + "]";
            if (av == null) {
                out.add(new DriftFinding(childPath, DriftFinding.Kind.ADDED,
                        null, toRendered(bv), section));
            } else if (bv == null) {
                out.add(new DriftFinding(childPath, DriftFinding.Kind.REMOVED,
                        toRendered(av), null, section));
            } else {
                diffInto(av, bv, childPath, section, out);
            }
        }
    }

    /* ============================== helpers ============================== */

    private static boolean isMap(Object o) { return o instanceof Map<?, ?>; }

    private static boolean equalScalars(Object a, Object b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        return a.equals(b);
    }

    private static String sectionOf(String topKey) {
        return switch (topKey) {
            case "users" -> "users";
            case "roles" -> "roles";
            default -> topKey;
        };
    }

    private static String toRendered(Object v) {
        if (v == null) return "";
        if (v instanceof Map<?, ?> m) {
            TreeSet<String> keys = new TreeSet<>();
            for (Object k : m.keySet()) keys.add(String.valueOf(k));
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (String k : keys) {
                if (!first) sb.append(", ");
                first = false;
                sb.append(k).append(": ").append(toRendered(m.get(k)));
            }
            return sb.append("}").toString();
        }
        if (v instanceof List<?> l) {
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (Object el : l) {
                if (!first) sb.append(", ");
                first = false;
                sb.append(toRendered(el));
            }
            return sb.append("]").toString();
        }
        return String.valueOf(v);
    }
}
