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

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(ConfigSnapshotService.class);

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

        // Parameters — getParameter(*: 1) returns every param. A
        // locked-down server may refuse; log at debug so an operator
        // can see why a capture came up short without scaring the
        // user with an error toast.
        try {
            Document reply = admin.runCommand(new Document("getParameter", "*"));
            ids.add(persist(connectionId, host, ConfigSnapshot.Kind.PARAMETERS,
                    canonicalize(sanitize(reply)), now));
        } catch (Exception e) {
            log.debug("PARAMETERS capture failed on {}/{}: {}",
                    connectionId, host, e.getMessage());
        }

        // Command line — redacted.
        try {
            Document reply = admin.runCommand(new Document("getCmdLineOpts", 1));
            ids.add(persist(connectionId, host, ConfigSnapshot.Kind.CMDLINE,
                    canonicalize(redactCmdLine(reply)), now));
        } catch (Exception e) {
            log.debug("CMDLINE capture failed on {}/{}: {}",
                    connectionId, host, e.getMessage());
        }

        // FCV — getParameter: featureCompatibilityVersion.
        try {
            Document reply = admin.runCommand(new Document("getParameter", 1)
                    .append("featureCompatibilityVersion", 1));
            ids.add(persist(connectionId, /*host=*/null,
                    ConfigSnapshot.Kind.FCV,
                    canonicalize(reply), now));
        } catch (Exception e) {
            log.debug("FCV capture failed on {}: {}",
                    connectionId, e.getMessage());
        }

        // Sharding — balancerStatus when the cluster is sharded.
        // Replica-set / standalone: command is unknown, which is the
        // benign "not a sharded cluster" signal — no log.
        try {
            Document reply = admin.runCommand(new Document("balancerStatus", 1));
            ids.add(persist(connectionId, /*host=*/null,
                    ConfigSnapshot.Kind.SHARDING,
                    canonicalize(reply), now));
        } catch (com.mongodb.MongoCommandException mce) {
            if (mce.getErrorCode() != 59 /* CommandNotFound */) {
                log.debug("SHARDING capture failed on {}: {}",
                        connectionId, mce.getMessage());
            }
        } catch (Exception e) {
            log.debug("SHARDING capture failed on {}: {}",
                    connectionId, e.getMessage());
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

    /** Exact-match denylist of config keys that carry secret-adjacent
     *  values. Earlier draft used a {@code contains("key")} substring
     *  test which wrongly redacted legitimate fields like
     *  {@code shardKey}, {@code primaryKey}, {@code indexKey} — the
     *  drift engine keys on SHA-256 of the canonical JSON so blanket
     *  redaction collapsed real changes on those fields into identity
     *  hashes. An exact denylist keeps the false-positive rate at 0
     *  while still redacting the fields that actually matter. */
    private static final java.util.Set<String> SENSITIVE_KEYS = java.util.Set.of(
            "password", "passwd", "secret", "secretkey",
            "token", "authtoken", "bearertoken", "apikey", "apisecret",
            "keyfile", "keyfilepassword",
            "awssecretaccesskey", "awssessiontoken",
            "tlsclientcertpassword", "tlscertificatekeyfilepassword",
            "ldapbindpassword", "ldapquerypassword",
            "kmipclientcertificatepassword",
            "pemkeyfilepassword", "clusterkeypassword",
            "encryptionkeyfile");

    private static boolean looksSensitive(String key) {
        return SENSITIVE_KEYS.contains(key.toLowerCase());
    }

    /** Canonicalise a map — used by tests to exercise the sort pass
     *  without building a real {@link Document}. */
    public static String canonicalizeForTest(Map<String, Object> m) {
        Document d = new Document(new LinkedHashMap<>(m));
        return canonicalize(d);
    }
}
