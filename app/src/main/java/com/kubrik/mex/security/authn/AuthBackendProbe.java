package com.kubrik.mex.security.authn;

import com.kubrik.mex.core.MongoService;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * v2.6 Q2.6-F1 — reads which SASL mechanisms the server advertises and
 * folds in whatever LDAP / Kerberos config it exposes through
 * {@code getCmdLineOpts}. No secrets cross this boundary:
 *
 * <ul>
 *   <li>{@code authenticationMechanisms} is a public server-parameter —
 *       reading it needs only {@code clusterMonitor} or similar.</li>
 *   <li>{@code getCmdLineOpts.security} is where the LDAP bind DN, the
 *       Kerberos principal, and the TLS certificate file paths live.
 *       The probe copies <em>paths</em> and <em>identifiers</em> but
 *       explicitly drops any key whose name contains {@code password}
 *       or {@code secret}, matching {@link com.kubrik.mex.migration.log.Redactor}'s
 *       default list.</li>
 * </ul>
 *
 * <p>Parsing is extracted to a package-private {@code parse(...)} so the
 * full cross-product of mechanism + config shapes is unit-testable
 * without a live server.</p>
 */
public final class AuthBackendProbe {

    private static final Logger log = LoggerFactory.getLogger(AuthBackendProbe.class);

    /** Keys we never copy into {@link AuthBackend#details}, regardless of
     *  where they appear in the {@code security} subtree. Lower-case
     *  substring match so {@code bindPassword} / {@code ldap.bind.password}
     *  / {@code clusterAuthPassword} are all caught. */
    static final List<String> REDACT_SUBSTRINGS = List.of("password", "secret",
            "keyfile", "keypasswd", "keypassword");

    public record Snapshot(List<AuthBackend> backends, long probedAtMs) {
        public Snapshot {
            backends = backends == null ? List.of() : List.copyOf(backends);
        }
    }

    public Snapshot probe(MongoService svc) {
        Document mechParam = runSafe(svc,
                new Document("getParameter", 1).append("authenticationMechanisms", 1));
        Document cmdLine   = runSafe(svc, new Document("getCmdLineOpts", 1));
        Snapshot s = parse(mechParam, cmdLine);
        return new Snapshot(s.backends(), System.currentTimeMillis());
    }

    /** Package-private parser. Accepts either reply being {@code null};
     *  the probe returns whatever information is available (SCRAM is
     *  effectively always on even when getCmdLineOpts refuses). */
    static Snapshot parse(Document mechReply, Document cmdLineReply) {
        List<String> wireMechs = mechReply == null ? List.of()
                : mechReply.getList("authenticationMechanisms", String.class, List.of());

        EnumMap<AuthBackend.Mechanism, Boolean> enabled = new EnumMap<>(AuthBackend.Mechanism.class);
        for (String w : wireMechs) {
            AuthBackend.Mechanism m = AuthBackend.Mechanism.fromWire(w);
            if (m != null) enabled.put(m, true);
        }

        Map<String, String> ldap = extractSection(cmdLineReply, "ldap");
        Map<String, String> sasl = extractSection(cmdLineReply, "sasl");
        Map<String, String> kerberos = extractSection(cmdLineReply, "kerberos");
        Map<String, String> generalTls = extractSection(cmdLineReply, "tls");

        List<AuthBackend> out = new ArrayList<>();
        for (AuthBackend.Mechanism m : AuthBackend.Mechanism.values()) {
            boolean on = Boolean.TRUE.equals(enabled.get(m));
            out.add(new AuthBackend(m, on, detailsFor(m, ldap, sasl, kerberos, generalTls)));
        }
        return new Snapshot(out, 0L);
    }

    /* =========================== section extract =========================== */

    private static Map<String, String> detailsFor(AuthBackend.Mechanism m,
                                                    Map<String, String> ldap,
                                                    Map<String, String> sasl,
                                                    Map<String, String> kerberos,
                                                    Map<String, String> generalTls) {
        return switch (m) {
            case PLAIN_LDAP -> {
                Map<String, String> out = new LinkedHashMap<>();
                out.putAll(ldap);
                out.putAll(sasl);     // saslauthd hostAndPort etc.
                yield out;
            }
            case GSSAPI -> {
                Map<String, String> out = new LinkedHashMap<>();
                out.putAll(kerberos);
                yield out;
            }
            case MONGODB_X509 -> generalTls;  // certificateFile / CAFile paths
            case SCRAM_SHA_256, SCRAM_SHA_1 -> Map.of();
        };
    }

    /** Reads {@code security.<section>} from getCmdLineOpts, flattened to a
     *  dot-path map, with secret-looking keys removed. */
    private static Map<String, String> extractSection(Document cmdLineReply, String section) {
        if (cmdLineReply == null) return Map.of();
        Document parsed = cmdLineReply.get("parsed", Document.class);
        if (parsed == null) return Map.of();
        Document security = parsed.get("security", Document.class);
        if (security == null) return Map.of();
        Object node = security.get(section);
        Map<String, String> out = new LinkedHashMap<>();
        if (node == null) return out;
        flatten(section, node, out);
        return out;
    }

    @SuppressWarnings("unchecked")
    private static void flatten(String prefix, Object node, Map<String, String> out) {
        if (node instanceof Document d) {
            for (String k : d.keySet()) {
                String childKey = prefix.isEmpty() ? k : prefix + "." + k;
                flatten(childKey, d.get(k), out);
            }
        } else if (node instanceof Map<?, ?> m) {
            for (Map.Entry<?, ?> e : m.entrySet()) {
                String childKey = prefix.isEmpty() ? String.valueOf(e.getKey())
                        : prefix + "." + e.getKey();
                flatten(childKey, e.getValue(), out);
            }
        } else if (node instanceof List<?> l) {
            // Render lists as their string form — preserves admin intent
            // (e.g., LDAP server addresses) without exposing secrets.
            if (!isRedacted(prefix)) out.put(prefix, l.toString());
        } else {
            if (!isRedacted(prefix)) {
                out.put(prefix, node == null ? "" : String.valueOf(node));
            }
        }
    }

    static boolean isRedacted(String key) {
        String k = key.toLowerCase(java.util.Locale.ROOT);
        for (String r : REDACT_SUBSTRINGS) if (k.contains(r)) return true;
        return false;
    }

    /* =============================== helpers =============================== */

    private static Document runSafe(MongoService svc, Document cmd) {
        try {
            return svc.database("admin").runCommand(cmd);
        } catch (Exception e) {
            log.debug("{} failed: {}", cmd.keySet().iterator().next(), e.getMessage());
            return null;
        }
    }
}
