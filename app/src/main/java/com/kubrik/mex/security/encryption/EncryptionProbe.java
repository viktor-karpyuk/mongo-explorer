package com.kubrik.mex.security.encryption;

import com.kubrik.mex.core.MongoService;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * v2.6 Q2.6-G1 — reads {@code serverStatus} + {@code getCmdLineOpts} off a
 * connected {@link MongoService} and folds them into a single
 * {@link EncryptionStatus}. Per-node probing is a cluster-level concern
 * (loops over {@code TopologySnapshot.members}); this class covers the
 * single-host path and is the unit the CIS runner in Q2.6-H consumes.
 *
 * <p>Detection priority:
 * <ol>
 *   <li>{@code serverStatus.encryptionAtRest} — richest signal,
 *       includes keystore + rotation + cipher. WiredTiger Enterprise
 *       emits this when encryption is active.</li>
 *   <li>{@code getCmdLineOpts.parsed.security.enableEncryption} — boolean
 *       flag; we can tell encryption is <em>configured</em> but not
 *       whether it's currently live.</li>
 *   <li>Otherwise {@link EncryptionStatus} reports {@code enabled=false,
 *       keystore=NONE} and the CIS scan raises a finding.</li>
 * </ol>
 *
 * <p>Parsing is extracted to a package-private {@code parse(…)} so tests
 * feed fixture documents end-to-end without a live server.</p>
 */
public final class EncryptionProbe {

    private static final Logger log = LoggerFactory.getLogger(EncryptionProbe.class);

    public EncryptionStatus probe(MongoService svc, String host) {
        Document serverStatus = runSafe(svc, new Document("serverStatus", 1));
        Document cmdLine = runSafe(svc, new Document("getCmdLineOpts", 1));
        return parse(host, serverStatus, cmdLine);
    }

    static EncryptionStatus parse(String host, Document serverStatus, Document cmdLine) {
        Document section = serverStatus == null ? null
                : serverStatus.get("encryptionAtRest", Document.class);
        if (section != null) return fromServerStatus(host, section);

        if (cmdLine != null) {
            Document parsed = cmdLine.get("parsed", Document.class);
            Document security = parsed == null ? null : parsed.get("security", Document.class);
            if (security != null
                    && Boolean.TRUE.equals(security.getBoolean("enableEncryption"))) {
                return new EncryptionStatus(host, true, "wiredTiger",
                        keystoreFromCmdLine(security), null,
                        stringOrEmpty(security, "encryptionCipherMode"),
                        "getCmdLineOpts");
            }
        }
        return new EncryptionStatus(host, false, "", EncryptionStatus.Keystore.NONE,
                null, "", "serverStatus");
    }

    /* ============================ serverStatus ============================ */

    private static EncryptionStatus fromServerStatus(String host, Document section) {
        boolean enabled = Boolean.TRUE.equals(section.getBoolean("encryptionEnabled"))
                || Boolean.TRUE.equals(section.getBoolean("enabled"));

        String engine = stringOrEmpty(section, "engine");
        if (engine.isEmpty()) engine = "wiredTiger";

        EncryptionStatus.Keystore keystore = keystoreFromServerStatus(section);
        Long rotatedAtMs = rotationMs(section);
        String cipher = stringOrEmpty(section, "cipher");
        if (cipher.isEmpty()) cipher = stringOrEmpty(section, "cipherMode");

        return new EncryptionStatus(host, enabled, engine, keystore,
                rotatedAtMs, cipher, "serverStatus");
    }

    private static EncryptionStatus.Keystore keystoreFromServerStatus(Document s) {
        if (s.get("kmip") != null) return EncryptionStatus.Keystore.KMIP;
        if (s.get("vault") != null) return EncryptionStatus.Keystore.VAULT;
        if (s.get("localKeyFile") != null) return EncryptionStatus.Keystore.LOCAL_FILE;
        String kind = stringOrEmpty(s, "keyStoreType");
        if (!kind.isEmpty()) {
            String k = kind.toLowerCase(java.util.Locale.ROOT);
            if (k.contains("kmip")) return EncryptionStatus.Keystore.KMIP;
            if (k.contains("vault")) return EncryptionStatus.Keystore.VAULT;
            if (k.contains("local") || k.contains("file"))
                return EncryptionStatus.Keystore.LOCAL_FILE;
        }
        return EncryptionStatus.Keystore.UNKNOWN;
    }

    private static EncryptionStatus.Keystore keystoreFromCmdLine(Document security) {
        // kmip / vault / encryptionKeyFile fields on the CLI side.
        if (security.get("kmip") != null) return EncryptionStatus.Keystore.KMIP;
        if (security.get("vault") != null) return EncryptionStatus.Keystore.VAULT;
        String keyFile = stringOrEmpty(security, "encryptionKeyFile");
        if (!keyFile.isEmpty()) return EncryptionStatus.Keystore.LOCAL_FILE;
        return EncryptionStatus.Keystore.UNKNOWN;
    }

    private static Long rotationMs(Document s) {
        Object v = s.get("keyRotatedAt");
        if (v == null) v = s.get("lastRotated");
        if (v == null) v = s.get("rotatedAt");
        if (v instanceof Number n) return n.longValue();
        if (v instanceof java.util.Date d) return d.getTime();
        return null;
    }

    /* =============================== helpers ============================== */

    private static String stringOrEmpty(Document d, String key) {
        Object v = d.get(key);
        return v == null ? "" : String.valueOf(v);
    }

    private static Document runSafe(MongoService svc, Document cmd) {
        try {
            return svc.database("admin").runCommand(cmd);
        } catch (Exception e) {
            log.debug("{} failed: {}", cmd.keySet().iterator().next(), e.getMessage());
            return null;
        }
    }
}
