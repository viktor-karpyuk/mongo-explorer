package com.kubrik.mex.labs.k8s.templates;

import com.kubrik.mex.k8s.provision.AuthSpec;
import com.kubrik.mex.k8s.provision.BackupSpec;
import com.kubrik.mex.k8s.provision.MonitoringSpec;
import com.kubrik.mex.k8s.provision.OperatorId;
import com.kubrik.mex.k8s.provision.Profile;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.k8s.provision.ResourceSpec;
import com.kubrik.mex.k8s.provision.SchedulingSpec;
import com.kubrik.mex.k8s.provision.StorageSpec;
import com.kubrik.mex.k8s.provision.TlsSpec;
import com.kubrik.mex.k8s.provision.Topology;
import com.kubrik.mex.labs.k8s.model.LabK8sDistro;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * v2.8.1 Q2.8-N3 — Curated day-one catalogue of K8s Labs templates.
 *
 * <p>Decision 11 boundary: templates produce {@link ProvisionModel}
 * inputs only. Every pathway past "ProvisionModel → YAML → apply"
 * is the v2.8.1 production code, unchanged.</p>
 *
 * <p>Five shipped templates (from the handoff):</p>
 * <ol>
 *   <li><b>psmdb-rs3</b> — Percona 3-node RS, Dev defaults, no TLS.</li>
 *   <li><b>mco-rs3</b> — MCO 3-node RS, Dev defaults, no TLS.</li>
 *   <li><b>psmdb-sharded</b> — Percona sharded cluster (3 shards × 3
 *       replica starter), Prod profile, cert-manager TLS.</li>
 *   <li><b>psmdb-pbm-backup</b> — Percona RS5 + native PBM backup,
 *       Prod profile.</li>
 *   <li><b>mco-tls-keyfile</b> — MCO RS3 with operator-generated
 *       TLS + keyfile (the default keyfile internal auth is
 *       implicit in MCO's RS shape).</li>
 * </ol>
 *
 * <p>Every template supports both {@code minikube} and {@code k3d}.
 * Templates that assume cert-manager (psmdb-sharded) flag that in
 * their {@code tags} so the UI can warn the user to install
 * cert-manager first.</p>
 */
public final class LabK8sTemplateRegistry {

    private static final Set<LabK8sDistro> BOTH_DISTROS =
            Set.of(LabK8sDistro.MINIKUBE, LabK8sDistro.K3D);

    private final Map<String, LabK8sTemplate> byId = new LinkedHashMap<>();

    public LabK8sTemplateRegistry() { loadBuiltins(); }

    private void loadBuiltins() {
        register(psmdbRs3());
        register(mcoRs3());
        register(psmdbSharded());
        register(psmdbPbmBackup());
        register(mcoTlsKeyfile());
    }

    private void register(LabK8sTemplate t) {
        if (byId.putIfAbsent(t.id(), t) != null) {
            throw new IllegalStateException("duplicate template id: " + t.id());
        }
    }

    public List<LabK8sTemplate> all() {
        return List.copyOf(new ArrayList<>(byId.values()));
    }

    public LabK8sTemplate byIdOrThrow(String id) {
        LabK8sTemplate t = byId.get(id);
        if (t == null) throw new IllegalArgumentException("unknown template " + id);
        return t;
    }

    /* ============================ templates ============================ */

    static LabK8sTemplate psmdbRs3() {
        return new LabK8sTemplate(
                "psmdb-rs3",
                "Percona RS3 (Dev)",
                "3-node Percona Server for MongoDB replica set. Dev profile, "
                + "no TLS, operator-managed users. Quick-start for PSMDB.",
                BOTH_DISTROS,
                List.of("operator:PSMDB", "topology:RS3", "profile:Dev"),
                (clusterId, namespace, deploymentName) -> ProvisionModel.defaults(
                        clusterId, namespace, deploymentName)
                        .withOperator(OperatorId.PSMDB)
                        .withTopology(Topology.RS3)
                        .withMongoVersion("7.0")
                        .withTls(TlsSpec.off())
                        .withStorage(StorageSpec.devDefaults())
                        .withAuth(AuthSpec.defaults())
                        .withBackup(BackupSpec.none())
                        .withMonitoring(MonitoringSpec.devDefaults())
                        .withScheduling(SchedulingSpec.devDefaults()));
    }

    static LabK8sTemplate mcoRs3() {
        return new LabK8sTemplate(
                "mco-rs3",
                "MCO RS3 (Dev)",
                "3-node MongoDB Community Operator replica set. Dev profile, "
                + "no TLS, operator-managed users.",
                BOTH_DISTROS,
                List.of("operator:MCO", "topology:RS3", "profile:Dev"),
                (clusterId, namespace, deploymentName) -> ProvisionModel.defaults(
                        clusterId, namespace, deploymentName)
                        .withOperator(OperatorId.MCO)
                        .withTopology(Topology.RS3)
                        .withMongoVersion("7.0")
                        .withTls(TlsSpec.off())
                        .withStorage(StorageSpec.devDefaults())
                        .withAuth(AuthSpec.defaults())
                        .withBackup(BackupSpec.none())
                        .withMonitoring(MonitoringSpec.devDefaults())
                        .withScheduling(SchedulingSpec.devDefaults()));
    }

    static LabK8sTemplate psmdbSharded() {
        return new LabK8sTemplate(
                "psmdb-sharded",
                "Percona sharded (3×3, Prod)",
                "Percona Server for MongoDB sharded cluster — 3 shards × "
                + "3-replica starter + config replset + mongos. Prod profile "
                + "with cert-manager TLS. Requires cert-manager pre-installed.",
                BOTH_DISTROS,
                List.of("operator:PSMDB", "topology:SHARDED", "profile:Prod", "requires:cert-manager"),
                (clusterId, namespace, deploymentName) -> ProvisionModel.defaults(
                        clusterId, namespace, deploymentName)
                        .withOperator(OperatorId.PSMDB)
                        .withTopology(Topology.SHARDED)
                        .withProfile(Profile.PROD)
                        .withMongoVersion("7.0")
                        .withTls(TlsSpec.certManager("mongo-issuer"))
                        .withStorage(new StorageSpec(Optional.empty(), 20, 5))
                        .withAuth(AuthSpec.defaults())
                        .withBackup(BackupSpec.none())
                        .withMonitoring(MonitoringSpec.prodDefaults())
                        .withScheduling(SchedulingSpec.prodDefaults())
                        .withResources(ResourceSpec.prodDefaults()));
    }

    static LabK8sTemplate psmdbPbmBackup() {
        return new LabK8sTemplate(
                "psmdb-pbm-backup",
                "Percona RS5 + PBM backup",
                "Percona Server for MongoDB 5-node replica set with native "
                + "PBM backup enabled. Prod profile, cert-manager TLS. "
                + "Walkthrough-style curriculum for operator-native backup.",
                BOTH_DISTROS,
                List.of("operator:PSMDB", "topology:RS5", "profile:Prod", "feature:backup"),
                (clusterId, namespace, deploymentName) -> ProvisionModel.defaults(
                        clusterId, namespace, deploymentName)
                        .withOperator(OperatorId.PSMDB)
                        .withTopology(Topology.RS5)
                        .withProfile(Profile.PROD)
                        .withMongoVersion("7.0")
                        .withTls(TlsSpec.certManager("mongo-issuer"))
                        .withStorage(new StorageSpec(Optional.empty(), 20, 5))
                        .withAuth(AuthSpec.defaults())
                        .withBackup(new BackupSpec(BackupSpec.Mode.PSMDB_PBM))
                        .withMonitoring(MonitoringSpec.prodDefaults())
                        .withScheduling(SchedulingSpec.prodDefaults())
                        .withResources(ResourceSpec.prodDefaults()));
    }

    static LabK8sTemplate mcoTlsKeyfile() {
        return new LabK8sTemplate(
                "mco-tls-keyfile",
                "MCO RS3 + TLS + keyfile",
                "MongoDB Community Operator 3-node RS with operator-"
                + "generated TLS and keyfile internal auth (implicit in "
                + "MCO's RS shape). Starter for TLS + auth hardening.",
                BOTH_DISTROS,
                List.of("operator:MCO", "topology:RS3", "profile:Dev", "feature:tls"),
                (clusterId, namespace, deploymentName) -> ProvisionModel.defaults(
                        clusterId, namespace, deploymentName)
                        .withOperator(OperatorId.MCO)
                        .withTopology(Topology.RS3)
                        .withMongoVersion("7.0")
                        .withTls(TlsSpec.operatorGenerated())
                        .withStorage(StorageSpec.devDefaults())
                        .withAuth(AuthSpec.defaults())
                        .withBackup(BackupSpec.none())
                        .withMonitoring(MonitoringSpec.devDefaults())
                        .withScheduling(SchedulingSpec.devDefaults()));
    }

    /** Visible for tests — snapshot of the registered ids. */
    public List<String> ids() {
        return Collections.unmodifiableList(new ArrayList<>(byId.keySet()));
    }
}
