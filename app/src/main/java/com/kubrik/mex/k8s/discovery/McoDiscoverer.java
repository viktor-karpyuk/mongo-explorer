package com.kubrik.mex.k8s.discovery;

import com.kubrik.mex.k8s.model.DiscoveredMongo;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-B1 — Discovers {@code MongoDBCommunity} custom
 * resources (MongoDB Community Operator).
 *
 * <p>The MCO CRD is {@code mongodbcommunity.mongodbcommunity.mongodb.com/v1}.
 * We query it via the generic {@link CustomObjectsApi} rather than
 * generated stubs — the stubs land in Q2.8.1-E once the operator
 * adapter needs them, and reusing them here would leak CRD-version
 * coupling into the discovery path.</p>
 *
 * <p>Any operator-version drift is tolerated: we parse fields best-
 * effort and fall back to {@link DiscoveredMongo.Topology#UNKNOWN}
 * rather than throwing. A cluster where MCO isn't installed simply
 * returns an empty list (the CustomObjectsApi throws 404).</p>
 */
public final class McoDiscoverer {

    private static final Logger log = LoggerFactory.getLogger(McoDiscoverer.class);

    public static final String GROUP   = "mongodbcommunity.mongodb.com";
    public static final String VERSION = "v1";
    public static final String PLURAL  = "mongodbcommunity";

    /**
     * Enumerate every MCO CR in every namespace visible to the
     * client. A 404 on the CRD itself is swallowed as "operator not
     * installed" and yields an empty list; other API exceptions
     * propagate.
     */
    public List<DiscoveredMongo> discover(ApiClient client, long clusterId) throws ApiException {
        Objects.requireNonNull(client, "client");
        CustomObjectsApi api = new CustomObjectsApi(client);
        Object raw;
        try {
            raw = api.listClusterCustomObject(GROUP, VERSION, PLURAL).execute();
        } catch (ApiException ae) {
            if (ae.getCode() == 404) {
                log.debug("MCO CRD absent on cluster {}", clusterId);
                return List.of();
            }
            throw ae;
        }
        return parseList(raw, clusterId);
    }

    static List<DiscoveredMongo> parseList(Object raw, long clusterId) {
        if (!(raw instanceof Map<?, ?> rootMap)) return List.of();
        Object itemsRaw = rootMap.get("items");
        if (!(itemsRaw instanceof List<?> items)) return List.of();
        List<DiscoveredMongo> out = new ArrayList<>();
        for (Object item : items) {
            if (!(item instanceof Map<?, ?> cr)) continue;
            DiscoveredMongo d = parseOne(cr, clusterId);
            if (d != null) out.add(d);
        }
        return Collections.unmodifiableList(out);
    }

    private static DiscoveredMongo parseOne(Map<?, ?> cr, long clusterId) {
        Map<?, ?> meta = asMap(cr.get("metadata"));
        if (meta == null) return null;
        String ns = asString(meta.get("namespace"));
        String name = asString(meta.get("name"));
        if (ns == null || name == null) return null;

        Map<?, ?> spec = asMap(cr.get("spec"));
        int members = asInt(spec == null ? null : spec.get("members"), 0);
        DiscoveredMongo.Topology topology = switch (members) {
            case 1 -> DiscoveredMongo.Topology.STANDALONE;
            case 3 -> DiscoveredMongo.Topology.RS3;
            case 5 -> DiscoveredMongo.Topology.RS5;
            default -> DiscoveredMongo.Topology.UNKNOWN;
        };

        String version = spec == null ? null : asString(spec.get("version"));

        // Auth: MCO spec.security.authentication.modes lists enabled
        // SCRAM / X509 etc. Best-effort default to SCRAM when unset
        // since the operator makes SCRAM the live-cluster default.
        Map<?, ?> security = asMap(spec == null ? null : spec.get("security"));
        Map<?, ?> authentication = asMap(security == null ? null : security.get("authentication"));
        DiscoveredMongo.AuthKind authKind = parseMcoAuth(authentication);

        // MCO emits a Service named after the CR — same name is the
        // convention; port is 27017 unless the operator ships a
        // surface for overriding, which it doesn't today.
        String serviceName = name + "-svc";
        int port = 27017;

        // status.phase == "Running" + all members Ready is the MCO
        // "ready" signal. We only peek at phase here to keep parsing
        // version-tolerant; deeper parsing lives in Q2.8.1-E's
        // McoStatusParser.
        Map<?, ?> status = asMap(cr.get("status"));
        Optional<Boolean> ready = status == null
                ? Optional.empty()
                : Optional.of("Running".equalsIgnoreCase(asString(status.get("phase"))));

        return new DiscoveredMongo(
                clusterId,
                DiscoveredMongo.Origin.MCO,
                ns, name,
                topology,
                Optional.of(serviceName),
                Optional.of(port),
                authKind,
                ready,
                Optional.ofNullable(version),
                Optional.of(name),
                Optional.empty());
    }

    static DiscoveredMongo.AuthKind parseMcoAuth(Map<?, ?> authentication) {
        if (authentication == null) return DiscoveredMongo.AuthKind.SCRAM;
        Object modesRaw = authentication.get("modes");
        if (!(modesRaw instanceof List<?> modes) || modes.isEmpty()) {
            return DiscoveredMongo.AuthKind.SCRAM;
        }
        boolean x509 = false, scram = false;
        for (Object m : modes) {
            String s = asString(m);
            if (s == null) continue;
            String lower = s.toLowerCase();
            if (lower.contains("scram")) scram = true;
            else if (lower.contains("x509")) x509 = true;
        }
        if (x509 && !scram) return DiscoveredMongo.AuthKind.X509;
        if (scram) return DiscoveredMongo.AuthKind.SCRAM;
        return DiscoveredMongo.AuthKind.UNKNOWN;
    }

    private static Map<?, ?> asMap(Object o) { return o instanceof Map<?, ?> m ? m : null; }
    private static String asString(Object o) { return o == null ? null : o.toString(); }
    private static int asInt(Object o, int dflt) {
        if (o instanceof Number n) return n.intValue();
        if (o instanceof String s) {
            try { return Integer.parseInt(s); } catch (NumberFormatException ignored) {}
        }
        return dflt;
    }
}
