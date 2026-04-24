package com.kubrik.mex.k8s.operator.psmdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.kubrik.mex.k8s.compute.nodepool.NodePoolRenderer;
import com.kubrik.mex.k8s.operator.KubernetesManifests;
import com.kubrik.mex.k8s.provision.BackupSpec;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.k8s.provision.TlsSpec;
import com.kubrik.mex.k8s.provision.Topology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * v2.8.1 Q2.8.1-F1 — Renders a {@link ProvisionModel} into a
 * {@code PerconaServerMongoDB} CR + supporting manifests.
 *
 * <p>Richer surface than MCO:</p>
 * <ul>
 *   <li>Sharded-capable. When {@code topology = SHARDED} we emit
 *       {@code spec.sharding.enabled=true} + three replsets (the
 *       v2.8.1 blessed starter — milestone §9.7) + a config replset
 *       + mongos. The replset sizes come from
 *       {@link Topology#replicasPerReplset()}.</li>
 *   <li>Native PBM backup. {@code backup.mode = PSMDB_PBM} emits
 *       {@code spec.backup.enabled=true} + a {@code storages} skeleton
 *       the user fills in. No managed CronJob — the operator
 *       schedules via PBM internally.</li>
 *   <li>Users Secret is a first-class output since PSMDB needs
 *       every system user (clusterAdmin / userAdmin / clusterMonitor
 *       / backup) present at CR-apply time.</li>
 * </ul>
 *
 * <p>Shares the determinism contract with the MCO renderer — pure
 * function of the model, no clock / randomness / API calls.</p>
 */
public final class PsmdbCRRenderer {

    public static final String CRD_API_VERSION = "psmdb.percona.com/v1";
    public static final String CRD_KIND = "PerconaServerMongoDB";

    public static final String MEX_LABEL = "mex.provisioning/renderer";
    public static final String MEX_LABEL_VALUE = "mongo-explorer";

    /** Full Mongo image — PSMDB is tag-specific, operator ties a CR version to a specific image. */
    private static final String DEFAULT_PSMDB_IMAGE = "percona/percona-server-mongodb:7.0.8-5";

    private final ObjectMapper yamlMapper;

    public PsmdbCRRenderer() {
        this.yamlMapper = new ObjectMapper(new YAMLFactory()
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .disable(YAMLGenerator.Feature.SPLIT_LINES));
    }

    public KubernetesManifests render(ProvisionModel m) {
        Objects.requireNonNull(m, "model");
        List<KubernetesManifests.Manifest> docs = new ArrayList<>();

        // Users Secret is mandatory — operator refuses to bootstrap
        // without every system user present. Default to a placeholder
        // the user edits unless they've picked PROVIDE mode.
        docs.add(renderUsersSecret(m));

        Map<String, Object> cr = buildCr(m);
        String crYaml = toYaml(cr);
        docs.add(new KubernetesManifests.Manifest(CRD_KIND, m.deploymentName(), crYaml));

        if (m.tls().mode() == TlsSpec.Mode.BYO_SECRET) {
            docs.add(renderByoTlsPlaceholder(m));
        }
        if (m.scheduling().pdbEnabled() && m.topology() != Topology.STANDALONE) {
            docs.add(renderPdb(m));
        }
        return new KubernetesManifests(CRD_KIND, m.deploymentName(), crYaml, docs);
    }

    /* ============================ CR body ============================ */

    private Map<String, Object> buildCr(ProvisionModel m) {
        Map<String, Object> cr = new LinkedHashMap<>();
        cr.put("apiVersion", CRD_API_VERSION);
        cr.put("kind", CRD_KIND);
        cr.put("metadata", meta(m.deploymentName(), m.namespace()));
        cr.put("spec", buildSpec(m));
        return cr;
    }

    private Map<String, Object> buildSpec(ProvisionModel m) {
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("crVersion", "1.17.0");
        spec.put("image", psmdbImage(m));
        spec.put("allowUnsafeConfigurations", m.topology() == Topology.STANDALONE);
        spec.put("updateStrategy", "SmartUpdate");

        spec.put("secrets", Map.of("users", m.deploymentName() + "-secrets"));

        spec.put("tls", tls(m));
        spec.put("replsets", replsets(m));

        if (m.topology() == Topology.SHARDED) {
            spec.put("sharding", sharding(m));
        }

        spec.put("backup", backup(m));

        if (m.monitoring().serviceMonitor()) {
            spec.put("pmm", Map.of(
                    "enabled", true,
                    "image", "percona/pmm-client:2.41.2",
                    "serverHost", "monitoring-service"));
        }
        return spec;
    }

    private Map<String, Object> tls(ProvisionModel m) {
        Map<String, Object> tls = new LinkedHashMap<>();
        switch (m.tls().mode()) {
            case OFF:
                tls.put("mode", "disabled");
                break;
            case OPERATOR_GENERATED:
                tls.put("mode", "preferTLS");
                break;
            case CERT_MANAGER:
                tls.put("mode", "requireTLS");
                tls.put("issuerConf", Map.of(
                        "name", m.tls().certManagerIssuer().orElse("mongo-issuer"),
                        "kind", "Issuer",
                        "group", "cert-manager.io"));
                break;
            case BYO_SECRET:
                tls.put("mode", "requireTLS");
                // BYO: operator reads <cr-name>-ssl + <cr-name>-ssl-internal.
                // User supplies them; we emit a placeholder below.
                break;
        }
        return tls;
    }

    private List<Map<String, Object>> replsets(ProvisionModel m) {
        // Starter preset: SHARDED emits 3 shard replsets of size 3 each
        // (milestone §9.7). Non-sharded emits a single rs0 replset.
        List<Map<String, Object>> out = new ArrayList<>();
        if (m.topology() == Topology.SHARDED) {
            for (int i = 0; i < m.topology().shardCount(); i++) {
                out.add(shardReplset("rs" + i, m));
            }
        } else {
            out.add(singleReplset(m));
        }
        return out;
    }

    private Map<String, Object> shardReplset(String name, ProvisionModel m) {
        Map<String, Object> rs = new LinkedHashMap<>();
        rs.put("name", name);
        rs.put("size", m.topology().replicasPerReplset());
        rs.put("volumeSpec", volumeSpec(m.storage().dataSizeGib(), m));
        rs.put("affinity", antiAffinity(m));
        if (m.resources().hasDataRequests()) {
            rs.put("resources", resources(m));
        }
        NodePoolRenderer.mutate(rs, m.computeStrategy(), m.deploymentName() + "-" + name);
        return rs;
    }

    private Map<String, Object> singleReplset(ProvisionModel m) {
        Map<String, Object> rs = new LinkedHashMap<>();
        rs.put("name", "rs0");
        rs.put("size", m.topology().replicasPerReplset());
        rs.put("volumeSpec", volumeSpec(m.storage().dataSizeGib(), m));
        if (m.topology() != Topology.STANDALONE) {
            rs.put("affinity", antiAffinity(m));
        }
        if (m.resources().hasDataRequests()) {
            rs.put("resources", resources(m));
        }
        NodePoolRenderer.mutate(rs, m.computeStrategy(), m.deploymentName() + "-rs0");
        return rs;
    }

    private Map<String, Object> sharding(ProvisionModel m) {
        Map<String, Object> sharding = new LinkedHashMap<>();
        sharding.put("enabled", true);

        Map<String, Object> configsvr = new LinkedHashMap<>();
        configsvr.put("size", 3);
        configsvr.put("volumeSpec", volumeSpec(m.storage().configServerSizeGib(), m));
        configsvr.put("affinity", antiAffinity(m));
        if (m.resources().hasDataRequests()) configsvr.put("resources", resources(m));
        NodePoolRenderer.mutate(configsvr, m.computeStrategy(), m.deploymentName() + "-cfg");
        sharding.put("configsvrReplSet", configsvr);

        Map<String, Object> mongos = new LinkedHashMap<>();
        mongos.put("size", 2);
        if (m.resources().mongosCpuRequest().isPresent()
                && m.resources().mongosMemRequest().isPresent()) {
            mongos.put("resources", Map.of("requests", Map.of(
                    "cpu", m.resources().mongosCpuRequest().get(),
                    "memory", m.resources().mongosMemRequest().get())));
        }
        mongos.put("affinity", antiAffinity(m));
        NodePoolRenderer.mutate(mongos, m.computeStrategy(), m.deploymentName() + "-mongos");
        sharding.put("mongos", mongos);

        return sharding;
    }

    private Map<String, Object> backup(ProvisionModel m) {
        Map<String, Object> backup = new LinkedHashMap<>();
        if (m.backup().mode() == BackupSpec.Mode.PSMDB_PBM) {
            backup.put("enabled", true);
            backup.put("image", "percona/percona-backup-mongodb:2.5.0");
            // storages: skeleton — user fills the actual bucket + creds.
            Map<String, Object> s3 = new LinkedHashMap<>();
            s3.put("type", "s3");
            s3.put("s3", Map.of(
                    "bucket", "<fill-in>",
                    "region", "us-east-1",
                    "credentialsSecret", m.deploymentName() + "-backup-s3"));
            backup.put("storages", Map.of("default-s3", s3));
            backup.put("pitr", Map.of("enabled", true));
        } else {
            backup.put("enabled", false);
        }
        return backup;
    }

    private Map<String, Object> antiAffinity(ProvisionModel m) {
        if (!m.scheduling().topologySpread()) {
            return Map.of("antiAffinityTopologyKey", "none");
        }
        return Map.of("antiAffinityTopologyKey", "kubernetes.io/hostname");
    }

    private Map<String, Object> volumeSpec(int sizeGib, ProvisionModel m) {
        Map<String, Object> pvc = new LinkedHashMap<>();
        pvc.put("accessModes", List.of("ReadWriteOnce"));
        pvc.put("resources", Map.of("requests", Map.of("storage", sizeGib + "Gi")));
        m.storage().storageClass().ifPresent(sc -> pvc.put("storageClassName", sc));
        return Map.of("persistentVolumeClaim", pvc);
    }

    private Map<String, Object> resources(ProvisionModel m) {
        Map<String, Object> out = new LinkedHashMap<>();
        Map<String, Object> requests = new LinkedHashMap<>();
        m.resources().dataCpuRequest().ifPresent(v -> requests.put("cpu", v));
        m.resources().dataMemRequest().ifPresent(v -> requests.put("memory", v));
        if (!requests.isEmpty()) out.put("requests", requests);
        Map<String, Object> limits = new LinkedHashMap<>();
        m.resources().dataCpuLimit().ifPresent(v -> limits.put("cpu", v));
        m.resources().dataMemLimit().ifPresent(v -> limits.put("memory", v));
        if (!limits.isEmpty()) out.put("limits", limits);
        return out;
    }

    /* ============================ aux docs ============================ */

    private KubernetesManifests.Manifest renderUsersSecret(ProvisionModel m) {
        // Placeholder values — the operator will reject passwords with
        // unsafe characters; users edit before apply. Password mode =
        // PROVIDE + a real password flows the user value into the
        // USER_ADMIN + CLUSTER_ADMIN slots.
        String userAdminPassword = m.auth().passwordMode()
                        == com.kubrik.mex.k8s.provision.AuthSpec.PasswordMode.PROVIDE
                ? m.auth().providedPassword().orElse("<supply-at-apply>")
                : "<generated-at-apply>";

        Map<String, Object> s = new LinkedHashMap<>();
        s.put("apiVersion", "v1");
        s.put("kind", "Secret");
        s.put("metadata", meta(m.deploymentName() + "-secrets", m.namespace()));
        s.put("type", "Opaque");
        Map<String, String> stringData = new LinkedHashMap<>();
        stringData.put("MONGODB_BACKUP_USER", "backup");
        stringData.put("MONGODB_BACKUP_PASSWORD", "<generated>");
        stringData.put("MONGODB_CLUSTER_ADMIN_USER", "clusterAdmin");
        stringData.put("MONGODB_CLUSTER_ADMIN_PASSWORD", userAdminPassword);
        stringData.put("MONGODB_CLUSTER_MONITOR_USER", "clusterMonitor");
        stringData.put("MONGODB_CLUSTER_MONITOR_PASSWORD", "<generated>");
        stringData.put("MONGODB_USER_ADMIN_USER", m.auth().rootUsername());
        stringData.put("MONGODB_USER_ADMIN_PASSWORD", userAdminPassword);
        s.put("stringData", stringData);
        return new KubernetesManifests.Manifest(
                "Secret", m.deploymentName() + "-secrets", toYaml(s));
    }

    private KubernetesManifests.Manifest renderByoTlsPlaceholder(ProvisionModel m) {
        String name = m.tls().byoSecretName().orElse(m.deploymentName() + "-ssl");
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("apiVersion", "v1");
        s.put("kind", "Secret");
        s.put("metadata", meta(name, m.namespace()));
        s.put("type", "kubernetes.io/tls");
        s.put("stringData", Map.of(
                "tls.crt", "<replace-with-your-pem>",
                "tls.key", "<replace-with-your-pem>",
                "ca.crt", "<replace-with-your-pem>"));
        return new KubernetesManifests.Manifest("Secret", name, toYaml(s));
    }

    private KubernetesManifests.Manifest renderPdb(ProvisionModel m) {
        Map<String, Object> pdb = new LinkedHashMap<>();
        pdb.put("apiVersion", "policy/v1");
        pdb.put("kind", "PodDisruptionBudget");
        pdb.put("metadata", meta(m.deploymentName() + "-pdb", m.namespace()));
        pdb.put("spec", Map.of(
                "maxUnavailable", 1,
                "selector", Map.of("matchLabels", Map.of(
                        "app.kubernetes.io/instance", m.deploymentName(),
                        "app.kubernetes.io/name", "percona-server-mongodb"))));
        return new KubernetesManifests.Manifest(
                "PodDisruptionBudget", m.deploymentName() + "-pdb", toYaml(pdb));
    }

    /* ============================ helpers ============================ */

    private Map<String, Object> meta(String name, String namespace) {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("name", name);
        meta.put("namespace", namespace);
        meta.put("labels", Map.of(
                MEX_LABEL, MEX_LABEL_VALUE,
                "app.kubernetes.io/instance", name));
        return meta;
    }

    private String psmdbImage(@SuppressWarnings("unused") ProvisionModel m) {
        // v2.8.1 Alpha: fixed to a single blessed tag (milestone §7.8).
        // When mongo-version widens (v2.8.1 Beta), this looks up
        // against an operator-version→image-tag table keyed off the
        // model's mongoVersion; for now the parameter is retained so
        // the call-site stays stable across that future change.
        return DEFAULT_PSMDB_IMAGE;
    }

    private String toYaml(Object doc) {
        try {
            return yamlMapper.writeValueAsString(doc);
        } catch (IOException ioe) {
            throw new IllegalStateException("YAML render failed", ioe);
        }
    }
}
