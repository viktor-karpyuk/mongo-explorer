package com.kubrik.mex.cluster.safety;

import com.kubrik.mex.cluster.dryrun.CommandJson;
import com.kubrik.mex.cluster.dryrun.DryRunRenderer;
import org.bson.Document;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * v2.4 SAFE-OPS-2/3 — integrity gate for destructive dispatch.
 *
 * <p>Every dispatcher must call {@link #requireMatch} immediately before
 * sending the command to Mongo. The gate recomputes SHA-256 over the canonical
 * JSON of the BSON tree the dispatcher is about to send and compares it to
 * the hash captured at dry-run time (which the user saw on the preview
 * screen). Any mismatch → fail closed with a structured exception; the
 * caller must log {@code outcome=FAIL, server_message=preview_hash_mismatch}
 * in {@code ops_audit}.</p>
 */
public final class PreviewHashChecker {

    private PreviewHashChecker() {}

    /** Recompute the canonical JSON + SHA-256 for the given BSON tree. */
    public static String hashOf(Document bson) {
        Map<String, Object> tree = toPlainMap(bson);
        String json = CommandJson.render(tree);
        return DryRunRenderer.sha256(json);
    }

    /** Thrown when the hash at dispatch time does not match the one the user approved. */
    public static final class PreviewTamperedException extends RuntimeException {
        public final String expected;
        public final String actual;
        public PreviewTamperedException(String expected, String actual) {
            super("preview_hash_mismatch");
            this.expected = expected;
            this.actual = actual;
        }
    }

    /** Fail-closed assertion: throws {@link PreviewTamperedException} on mismatch. */
    public static void requireMatch(Document dispatched, String expectedHash) {
        String actual = hashOf(dispatched);
        if (!actual.equals(expectedHash)) {
            throw new PreviewTamperedException(expectedHash, actual);
        }
    }

    /* ----------------------------- helpers ----------------------------- */

    /**
     * BSON {@link Document} → plain {@link Map} tree in insertion order so the
     * canonical-JSON renderer can sort keys deterministically. Nested
     * documents and lists are recursed.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> toPlainMap(Document d) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : d.entrySet()) {
            out.put(e.getKey(), convert(e.getValue()));
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static Object convert(Object v) {
        if (v instanceof Document nested) return toPlainMap(nested);
        if (v instanceof Map<?, ?> m) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : m.entrySet()) {
                out.put(String.valueOf(e.getKey()), convert(e.getValue()));
            }
            return out;
        }
        if (v instanceof Iterable<?> it) {
            java.util.List<Object> list = new java.util.ArrayList<>();
            for (Object el : it) list.add(convert(el));
            return list;
        }
        return v;
    }
}
