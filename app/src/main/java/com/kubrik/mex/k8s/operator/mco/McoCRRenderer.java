package com.kubrik.mex.k8s.operator.mco;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.kubrik.mex.k8s.operator.KubernetesManifests;
import com.kubrik.mex.k8s.provision.BackupSpec;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.k8s.provision.TlsSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * v2.8.1 Q2.8.1-E2 — Renders a {@link ProvisionModel} into a
 * {@code MongoDBCommunity} CR + every supporting manifest.
 *
 * <p>Deterministic + pure: no clock reads, no randomness. A stable
 * field-order per document + Jackson's {@code SORT_PROPERTIES_ALPHABETICALLY}
 * off keeps byte-for-byte stability so
 * {@link com.kubrik.mex.cluster.safety.PreviewHashChecker} can sign
 * the preview into the typed-confirm step (milestone §4.1).</p>
 *
 * <p>Emitted documents (in apply order):</p>
 * <ol>
 *   <li><b>Password Secret</b> (only when the user supplied a
 *       password — GENERATE leans on the operator).</li>
 *   <li><b>BYO CA Secret</b> — only when TLS mode = BYO_SECRET,
 *       and we emit a placeholder user is expected to replace.</li>
 *   <li><b>MongoDBCommunity</b> CR.</li>
 *   <li><b>PodDisruptionBudget</b> — when scheduling.pdbEnabled.</li>
 *   <li><b>ServiceMonitor</b> — when monitoring.serviceMonitor.</li>
 *   <li><b>Managed PBM bundle</b> — {@code Secret} + {@code Deployment}
 *       + {@code CronJob} + {@code ConfigMap} — when backup.mode =
 *       MANAGED_PBM_CRONJOB.</li>
 * </ol>
 */
public final class McoCRRenderer {

    public static final String CRD_API_VERSION = "mongodbcommunity.mongodb.com/v1";
    public static final String CRD_KIND = "MongoDBCommunity";

    /** Label every Mongo-Explorer-rendered object carries so cleanup can find them. */
    public static final String MEX_LABEL = "mex.provisioning/renderer";
    public static final String MEX_LABEL_VALUE = "mongo-explorer";

    private final ObjectMapper yamlMapper;

    public McoCRRenderer() {
        // BLOCK_SCALAR + no starting "---" keeps outputs diff-friendly
        // for the dry-run panel. SPLIT_LINES off avoids Jackson's 80-col
        // line wrap which breaks downstream Kubernetes YAML parsers
        // when multi-line strings contain PEM blobs.
        this.yamlMapper = new ObjectMapper(new YAMLFactory()
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .disable(YAMLGenerator.Feature.SPLIT_LINES));
    }

    public KubernetesManifests render(ProvisionModel m) {
        Objects.requireNonNull(m, "model");
        List<KubernetesManifests.Manifest> docs = new ArrayList<>();

        docs.addAll(passwordSecret(m));
        docs.addAll(byoCaPlaceholder(m));

        Map<String, Object> cr = buildCr(m);
        String crYaml = toYaml(cr);
        docs.add(new KubernetesManifests.Manifest(CRD_KIND, m.deploymentName(), crYaml));

        if (m.scheduling().pdbEnabled()) {
            docs.add(renderPdb(m));
        }
        if (m.monitoring().serviceMonitor()) {
            docs.add(renderServiceMonitor(m));
        }
        if (m.backup().mode() == BackupSpec.Mode.MANAGED_PBM_CRONJOB) {
            docs.addAll(renderPbmBundle(m));
        }
        return new KubernetesManifests(CRD_KIND, m.deploymentName(), crYaml, docs);
    }

    /* ============================ CR body ============================ */

    private Map<String, Object> buildCr(ProvisionModel m) {
        Map<String, Object> cr = new LinkedHashMap<>();
        cr.put("apiVersion", CRD_API_VERSION);
        cr.put("kind", CRD_KIND);
        cr.put("metadata", meta(m.deploymentName(), m.namespace()));

        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("members", m.topology().replicasPerReplset());
        spec.put("type", "ReplicaSet");
        spec.put("version", m.mongoVersion());

        spec.put("security", security(m));
        spec.put("users", users(m));

        if (m.resources().hasDataRequests() || m.scheduling().topologySpread()) {
            spec.put("statefulSet", statefulSetSpec(m));
        }

        if (m.monitoring().serviceMonitor()) {
            // MCO doesn't emit a ServiceMonitor itself; we render a
            // separate manifest below. Still stamp the label the
            // SM selector will match.
            spec.put("additionalMongodConfig", Map.of("setParameter", Map.of(
                    "diagnosticDataCollectionEnabled", "true")));
        }

        cr.put("spec", spec);
        return cr;
    }

    private Map<String, Object> security(ProvisionModel m) {
        Map<String, Object> security = new LinkedHashMap<>();
        // SCRAM-SHA-256 is the MCO default; lock it in explicitly
        // so we don't inherit surprise behaviour from an older
        // operator version.
        security.put("authentication",
                Map.of("modes", List.of("SCRAM-SHA-256")));

        switch (m.tls().mode()) {
            case OFF:
                // MCO pre-1.0 required tls.enabled=false; modern
                // versions default off. Emit explicit false so the
                // CR is self-describing.
                security.put("tls", Map.of("enabled", false));
                break;
            case OPERATOR_GENERATED:
                security.put("tls", Map.of("enabled", true));
                break;
            case CERT_MANAGER:
                Map<String, Object> tls = new LinkedHashMap<>();
                tls.put("enabled", true);
                tls.put("certificateKeySecretRef", Map.of("name",
                        m.deploymentName() + "-cert"));
                tls.put("caCertificateSecretRef", Map.of("name",
                        m.deploymentName() + "-ca"));
                tls.put("optional", false);
                security.put("tls", tls);
                break;
            case BYO_SECRET:
                Map<String, Object> byo = new LinkedHashMap<>();
                byo.put("enabled", true);
                String secretName = m.tls().byoSecretName()
                        .orElse(m.deploymentName() + "-tls");
                byo.put("certificateKeySecretRef", Map.of("name", secretName));
                byo.put("optional", false);
                security.put("tls", byo);
                break;
        }
        return security;
    }

    private List<Map<String, Object>> users(ProvisionModel m) {
        Map<String, Object> user = new LinkedHashMap<>();
        user.put("name", m.auth().rootUsername());
        user.put("db", "admin");
        user.put("passwordSecretRef", Map.of("name", m.deploymentName() + "-admin-user"));
        user.put("roles", List.of(
                Map.of("name", "root", "db", "admin"),
                Map.of("name", "clusterAdmin", "db", "admin")));
        user.put("scramCredentialsSecretName",
                m.deploymentName() + "-admin-scram");
        return List.of(user);
    }

    private Map<String, Object> statefulSetSpec(ProvisionModel m) {
        Map<String, Object> sts = new LinkedHashMap<>();
        Map<String, Object> spec = new LinkedHashMap<>();
        Map<String, Object> template = new LinkedHashMap<>();
        Map<String, Object> pod = new LinkedHashMap<>();
        // mongod container
        if (m.resources().hasDataRequests()) {
            Map<String, Object> container = new LinkedHashMap<>();
            container.put("name", "mongod");
            container.put("resources", resourceMap(
                    m.resources().dataCpuRequest().orElse(null),
                    m.resources().dataMemRequest().orElse(null),
                    m.resources().dataCpuLimit().orElse(null),
                    m.resources().dataMemLimit().orElse(null)));
            pod.put("containers", List.of(container));
        }
        if (m.scheduling().topologySpread()) {
            Map<String, Object> tsc = new LinkedHashMap<>();
            tsc.put("maxSkew", 1);
            tsc.put("topologyKey", "topology.kubernetes.io/zone");
            tsc.put("whenUnsatisfiable", "ScheduleAnyway");
            tsc.put("labelSelector", Map.of("matchLabels",
                    Map.of("app", m.deploymentName() + "-svc")));
            pod.put("topologySpreadConstraints", List.of(tsc));
        }
        template.put("spec", pod);
        spec.put("template", template);
        // Storage class + PVC size as volumeClaimTemplate
        Map<String, Object> vct = volumeClaimTemplate(m);
        if (vct != null) spec.put("volumeClaimTemplates", List.of(vct));
        sts.put("spec", spec);
        return sts;
    }

    private Map<String, Object> volumeClaimTemplate(ProvisionModel m) {
        Map<String, Object> vct = new LinkedHashMap<>();
        vct.put("metadata", Map.of("name", "data-volume"));
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("accessModes", List.of("ReadWriteOnce"));
        Map<String, Object> resources = new LinkedHashMap<>();
        resources.put("requests", Map.of("storage", m.storage().dataSizeGib() + "Gi"));
        spec.put("resources", resources);
        m.storage().storageClass().ifPresent(sc -> spec.put("storageClassName", sc));
        vct.put("spec", spec);
        return vct;
    }

    /* ============================ aux docs ============================ */

    private List<KubernetesManifests.Manifest> passwordSecret(ProvisionModel m) {
        if (m.auth().passwordMode() != com.kubrik.mex.k8s.provision.AuthSpec.PasswordMode.PROVIDE) {
            return List.of();
        }
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("apiVersion", "v1");
        s.put("kind", "Secret");
        s.put("metadata", meta(m.deploymentName() + "-admin-user", m.namespace()));
        s.put("type", "Opaque");
        s.put("stringData", Map.of("password",
                m.auth().providedPassword().orElse("<supply-at-apply>")));
        String name = m.deploymentName() + "-admin-user";
        return List.of(new KubernetesManifests.Manifest("Secret", name, toYaml(s)));
    }

    private List<KubernetesManifests.Manifest> byoCaPlaceholder(ProvisionModel m) {
        if (m.tls().mode() != TlsSpec.Mode.BYO_SECRET) return List.of();
        String name = m.tls().byoSecretName().orElse(m.deploymentName() + "-tls");
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("apiVersion", "v1");
        s.put("kind", "Secret");
        s.put("metadata", meta(name, m.namespace()));
        s.put("type", "kubernetes.io/tls");
        s.put("stringData", Map.of(
                "tls.crt", "<replace-with-your-pem>",
                "tls.key", "<replace-with-your-pem>",
                "ca.crt", "<replace-with-your-pem>"));
        return List.of(new KubernetesManifests.Manifest("Secret", name, toYaml(s)));
    }

    private KubernetesManifests.Manifest renderPdb(ProvisionModel m) {
        Map<String, Object> pdb = new LinkedHashMap<>();
        pdb.put("apiVersion", "policy/v1");
        pdb.put("kind", "PodDisruptionBudget");
        pdb.put("metadata", meta(m.deploymentName() + "-pdb", m.namespace()));
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("maxUnavailable", 1);
        spec.put("selector", Map.of("matchLabels",
                Map.of("app", m.deploymentName() + "-svc")));
        pdb.put("spec", spec);
        return new KubernetesManifests.Manifest(
                "PodDisruptionBudget", m.deploymentName() + "-pdb", toYaml(pdb));
    }

    private KubernetesManifests.Manifest renderServiceMonitor(ProvisionModel m) {
        Map<String, Object> sm = new LinkedHashMap<>();
        sm.put("apiVersion", "monitoring.coreos.com/v1");
        sm.put("kind", "ServiceMonitor");
        sm.put("metadata", meta(m.deploymentName() + "-servicemonitor", m.namespace()));
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("endpoints", List.of(Map.of(
                "port", "mongodb",
                "interval", "30s",
                "scheme", m.tls().mode() == TlsSpec.Mode.OFF ? "http" : "https")));
        spec.put("namespaceSelector", Map.of("matchNames", List.of(m.namespace())));
        spec.put("selector", Map.of("matchLabels",
                Map.of("app", m.deploymentName() + "-svc")));
        sm.put("spec", spec);
        return new KubernetesManifests.Manifest(
                "ServiceMonitor", m.deploymentName() + "-servicemonitor", toYaml(sm));
    }

    private List<KubernetesManifests.Manifest> renderPbmBundle(ProvisionModel m) {
        // Managed PBM CronJob bundle. Alpha emits a skeleton that
        // the user wires storage creds into — full S3 / GCS /
        // Azure permutations land with Q2.8.1-F/L alongside the
        // PSMDB backup wiring.
        List<KubernetesManifests.Manifest> out = new ArrayList<>();
        String base = m.deploymentName() + "-pbm";

        Map<String, Object> cfg = new LinkedHashMap<>();
        cfg.put("apiVersion", "v1");
        cfg.put("kind", "ConfigMap");
        cfg.put("metadata", meta(base + "-config", m.namespace()));
        cfg.put("data", Map.of("pbm.conf",
                "storage:\n  type: s3\n  s3:\n    region: <fill-in>\n    bucket: <fill-in>\n"));
        out.add(new KubernetesManifests.Manifest("ConfigMap", base + "-config", toYaml(cfg)));

        Map<String, Object> sec = new LinkedHashMap<>();
        sec.put("apiVersion", "v1");
        sec.put("kind", "Secret");
        sec.put("metadata", meta(base + "-storage-creds", m.namespace()));
        sec.put("type", "Opaque");
        sec.put("stringData", Map.of(
                "AWS_ACCESS_KEY_ID", "<replace>",
                "AWS_SECRET_ACCESS_KEY", "<replace>"));
        out.add(new KubernetesManifests.Manifest(
                "Secret", base + "-storage-creds", toYaml(sec)));

        Map<String, Object> container = new LinkedHashMap<>();
        container.put("name", "pbm");
        container.put("image", "percona/percona-backup-mongodb:2.5.0");
        container.put("command", List.of("sh", "-c",
                "pbm config --file=/etc/pbm/pbm.conf && pbm backup --type=logical"));
        Map<String, Object> podSpec = new LinkedHashMap<>();
        podSpec.put("restartPolicy", "OnFailure");
        podSpec.put("containers", List.of(container));
        Map<String, Object> jobTpl = new LinkedHashMap<>();
        jobTpl.put("spec", Map.of("template", Map.of("spec", podSpec)));
        Map<String, Object> cronSpec = new LinkedHashMap<>();
        cronSpec.put("schedule", "0 2 * * *");
        cronSpec.put("jobTemplate", jobTpl);
        Map<String, Object> cron = new LinkedHashMap<>();
        cron.put("apiVersion", "batch/v1");
        cron.put("kind", "CronJob");
        cron.put("metadata", meta(base + "-daily", m.namespace()));
        cron.put("spec", cronSpec);
        out.add(new KubernetesManifests.Manifest("CronJob", base + "-daily", toYaml(cron)));
        return out;
    }

    /* ============================ helpers ============================ */

    private Map<String, Object> meta(String name, String namespace) {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("name", name);
        meta.put("namespace", namespace);
        meta.put("labels", Map.of(
                MEX_LABEL, MEX_LABEL_VALUE,
                "app", name));
        return meta;
    }

    private Map<String, Object> resourceMap(String cpuReq, String memReq,
                                              String cpuLim, String memLim) {
        Map<String, Object> r = new LinkedHashMap<>();
        Map<String, Object> requests = new LinkedHashMap<>();
        if (cpuReq != null) requests.put("cpu", cpuReq);
        if (memReq != null) requests.put("memory", memReq);
        if (!requests.isEmpty()) r.put("requests", requests);
        Map<String, Object> limits = new LinkedHashMap<>();
        if (cpuLim != null) limits.put("cpu", cpuLim);
        if (memLim != null) limits.put("memory", memLim);
        if (!limits.isEmpty()) r.put("limits", limits);
        return r;
    }

    private String toYaml(Object doc) {
        try {
            return yamlMapper.writeValueAsString(doc);
        } catch (IOException ioe) {
            throw new IllegalStateException("YAML render failed", ioe);
        }
    }

}
