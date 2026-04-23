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
 * v2.8.1 Q2.8.1-B1 — Discovers {@code PerconaServerMongoDB} custom
 * resources (Percona Server for MongoDB Operator).
 *
 * <p>Richer CR shape than MCO — supports sharding, multiple
 * replsets, and arbiters. For v2.8.1 Alpha we surface the common
 * cases: sharded → {@link DiscoveredMongo.Topology#SHARDED}, a
 * single replset with size 3 / 5 → {@code RS3} / {@code RS5},
 * size 1 → {@code STANDALONE}. Anything else falls to
 * {@code UNKNOWN} — the operator adapter lands with Q2.8.1-F and
 * will parse the full surface.</p>
 */
public final class PsmdbDiscoverer {

    private static final Logger log = LoggerFactory.getLogger(PsmdbDiscoverer.class);

    public static final String GROUP   = "psmdb.percona.com";
    public static final String VERSION = "v1";
    public static final String PLURAL  = "perconaservermongodbs";

    public List<DiscoveredMongo> discover(ApiClient client, long clusterId) throws ApiException {
        Objects.requireNonNull(client, "client");
        CustomObjectsApi api = new CustomObjectsApi(client);
        Object raw;
        try {
            raw = api.listClusterCustomObject(GROUP, VERSION, PLURAL).execute();
        } catch (ApiException ae) {
            if (ae.getCode() == 404) {
                log.debug("PSMDB CRD absent on cluster {}", clusterId);
                return List.of();
            }
            throw ae;
        }
        return parseList(raw, clusterId);
    }

    static List<DiscoveredMongo> parseList(Object raw, long clusterId) {
        if (!(raw instanceof Map<?, ?> root)) return List.of();
        Object itemsRaw = root.get("items");
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
        boolean sharded = isSharded(spec);
        DiscoveredMongo.Topology topology = sharded
                ? DiscoveredMongo.Topology.SHARDED
                : topologyFromReplsets(spec);

        String version = spec == null ? null : asString(spec.get("image"));
        // image string is full tag; Q2.8.1-F will refine to version only.

        // PSMDB auth defaults to SCRAM (users Secret); X.509 is
        // opt-in under spec.tls / spec.users. Treat SCRAM as the
        // default surface; fine-grained detection lands with the
        // secret resolver in B2.
        DiscoveredMongo.AuthKind authKind = DiscoveredMongo.AuthKind.SCRAM;

        // Service name convention: <name>-rs0 for an RS; <name>-mongos
        // for a sharded cluster. The wizard's connect-to-existing
        // flow will reconcile this against the actual Service list.
        String svcName = sharded ? name + "-mongos" : name + "-rs0";

        Map<?, ?> status = asMap(cr.get("status"));
        Optional<Boolean> ready = status == null
                ? Optional.empty()
                : Optional.of("ready".equalsIgnoreCase(asString(status.get("state"))));

        return new DiscoveredMongo(
                clusterId,
                DiscoveredMongo.Origin.PSMDB,
                ns, name,
                topology,
                Optional.of(svcName),
                Optional.of(27017),
                authKind,
                ready,
                Optional.ofNullable(version),
                Optional.of(name),
                Optional.empty());
    }

    private static boolean isSharded(Map<?, ?> spec) {
        if (spec == null) return false;
        Object sharding = spec.get("sharding");
        if (!(sharding instanceof Map<?, ?> shardingMap)) return false;
        Object enabled = shardingMap.get("enabled");
        return Boolean.TRUE.equals(enabled);
    }

    private static DiscoveredMongo.Topology topologyFromReplsets(Map<?, ?> spec) {
        if (spec == null) return DiscoveredMongo.Topology.UNKNOWN;
        Object rs = spec.get("replsets");
        if (!(rs instanceof List<?> list) || list.isEmpty()) {
            return DiscoveredMongo.Topology.UNKNOWN;
        }
        // For v2.8.1 we classify on the first replset's size. Multi-
        // replset non-sharded configs are rare and land as UNKNOWN
        // via this path too.
        Object first = list.get(0);
        if (!(first instanceof Map<?, ?> rs0)) return DiscoveredMongo.Topology.UNKNOWN;
        int size = asInt(rs0.get("size"), 0);
        return switch (size) {
            case 1 -> DiscoveredMongo.Topology.STANDALONE;
            case 3 -> DiscoveredMongo.Topology.RS3;
            case 5 -> DiscoveredMongo.Topology.RS5;
            default -> DiscoveredMongo.Topology.UNKNOWN;
        };
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
