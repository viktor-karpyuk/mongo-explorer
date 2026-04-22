package com.kubrik.mex.maint.drift;

import com.kubrik.mex.maint.model.ConfigSnapshot;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * v2.7 DRIFT-CFG-1/2 — Captures config snapshots. Collects
 * cluster-wide parameters, FCV, host-level {@code getCmdLineOpts}
 * (redacted), and sharding settings.
 *
 * <p>Canonical JSON: keys sorted alphabetically so two capture runs
 * on an unchanged config produce byte-identical JSON and the SHA-256
 * hash collapses them into a single diff-engine node.</p>
 */
public final class ConfigSnapshotService {

    private final ConfigSnapshotDao dao;

    public ConfigSnapshotService(ConfigSnapshotDao dao) {
        this.dao = dao;
    }

    /** Capture every snapshot kind the cluster supports. Returns the
     *  row IDs in insertion order. */
    public java.util.List<Long> captureAll(MongoClient client,
                                           String connectionId,
                                           String host,
                                           long now) {
        java.util.List<Long> ids = new java.util.ArrayList<>(4);
        MongoDatabase admin = client.getDatabase("admin");

        // Parameters — getParameter(*: 1) returns every param.
        try {
            Document reply = admin.runCommand(new Document("getParameter", "*"));
            ids.add(persist(connectionId, host, ConfigSnapshot.Kind.PARAMETERS,
                    canonicalize(sanitize(reply)), now));
        } catch (Exception ignored) {}

        // Command line — redacted.
        try {
            Document reply = admin.runCommand(new Document("getCmdLineOpts", 1));
            ids.add(persist(connectionId, host, ConfigSnapshot.Kind.CMDLINE,
                    canonicalize(redactCmdLine(reply)), now));
        } catch (Exception ignored) {}

        // FCV — getParameter: featureCompatibilityVersion.
        try {
            Document reply = admin.runCommand(new Document("getParameter", 1)
                    .append("featureCompatibilityVersion", 1));
            ids.add(persist(connectionId, /*host=*/null,
                    ConfigSnapshot.Kind.FCV,
                    canonicalize(reply), now));
        } catch (Exception ignored) {}

        // Sharding — balancerStatus when the cluster is sharded.
        try {
            Document reply = admin.runCommand(new Document("balancerStatus", 1));
            ids.add(persist(connectionId, /*host=*/null,
                    ConfigSnapshot.Kind.SHARDING,
                    canonicalize(reply), now));
        } catch (Exception ignored) {
            // Replica-set / standalone: balancerStatus is a no-op.
        }

        return ids;
    }

    private long persist(String connectionId, String host,
                         ConfigSnapshot.Kind kind, String json, long now) {
        String sha = ConfigSnapshotDao.sha256(json);
        return dao.insert(new ConfigSnapshot(-1, connectionId, now, host,
                kind, json, sha));
    }

    /** Sort keys alphabetically recursively. Document ordering is
     *  preserved by BSON; canonical form needs a TreeMap pass for
     *  the stored form to be stable across captures. */
    static String canonicalize(Document doc) {
        Map<String, Object> sorted = sortDeep(doc);
        Document flat = new Document(sorted);
        return flat.toJson(JsonWriterSettings.builder().indent(false).build());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> sortDeep(Document d) {
        Map<String, Object> out = new TreeMap<>();
        for (Map.Entry<String, Object> e : d.entrySet()) {
            Object v = e.getValue();
            if (v instanceof Document sub) v = sortDeep(sub);
            out.put(e.getKey(), v);
        }
        return out;
    }

    /** Drop wildcard-asterisk keys + any value that looks like a
     *  secret. Hobbits-in-the-pipeline: if a value's key contains
     *  "password" / "secret" / "token" / "key", redact. */
    static Document sanitize(Document d) {
        Document out = new Document();
        for (Map.Entry<String, Object> e : d.entrySet()) {
            String k = e.getKey();
            if (looksSensitive(k)) {
                out.put(k, "<redacted>");
                continue;
            }
            Object v = e.getValue();
            if (v instanceof Document sub) v = sanitize(sub);
            out.put(k, v);
        }
        return out;
    }

    /** {@code getCmdLineOpts} reports the parsed argv; redact any
     *  value under a key that carries secrets. */
    @SuppressWarnings("unchecked")
    static Document redactCmdLine(Document d) {
        Document out = new Document();
        for (Map.Entry<String, Object> e : d.entrySet()) {
            Object v = e.getValue();
            if (v instanceof Document sub) v = redactCmdLine(sub);
            else if (v instanceof java.util.List<?> l
                    && !l.isEmpty() && l.get(0) instanceof Document) {
                java.util.List<Document> mapped = new java.util.ArrayList<>();
                for (Document sub : (java.util.List<Document>) l) {
                    mapped.add(redactCmdLine(sub));
                }
                v = mapped;
            } else if (v instanceof String s && looksSensitive(e.getKey())) {
                v = "<redacted>";
            }
            out.put(e.getKey(), v);
        }
        return out;
    }

    private static boolean looksSensitive(String key) {
        String k = key.toLowerCase();
        // "keyFile" / "tlsCAFile" / etc. — any key containing one of
        // these substrings is treated as secret-adjacent and
        // redacted. Over-redaction is fine for snapshot purposes
        // (the diff engine compares hashes, not values).
        return k.contains("password") || k.contains("secret")
                || k.contains("token") || k.contains("key");
    }

    /** Canonicalise a map — used by tests to exercise the sort pass
     *  without building a real {@link Document}. */
    public static String canonicalizeForTest(Map<String, Object> m) {
        Document d = new Document(new LinkedHashMap<>(m));
        return canonicalize(d);
    }
}
