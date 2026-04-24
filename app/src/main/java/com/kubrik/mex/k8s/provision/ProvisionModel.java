package com.kubrik.mex.k8s.provision;

import com.kubrik.mex.k8s.compute.ComputeStrategy;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-D1 — Wizard state holder.
 *
 * <p>Carries every user choice across the wizard's 14 steps
 * (milestone §2.5). Pure record: no API calls, no enforcement —
 * {@link ProfileEnforcer} is the single source of truth for what
 * each value enforces.</p>
 *
 * <p>Record is deeply immutable; every {@code with…} helper returns
 * a new instance. Used both for the live wizard state and for
 * clone / export flows (Q2.8.1-J).</p>
 */
public record ProvisionModel(
        Profile profile,
        long clusterId,
        String namespace,
        boolean createNamespace,
        String deploymentName,
        OperatorId operator,
        Topology topology,
        String mongoVersion,
        AuthSpec auth,
        TlsSpec tls,
        StorageSpec storage,
        ResourceSpec resources,
        SchedulingSpec scheduling,
        MonitoringSpec monitoring,
        BackupSpec backup,
        Optional<String> advancedYaml,
        ComputeStrategy computeStrategy) {

    public ProvisionModel {
        Objects.requireNonNull(profile, "profile");
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(deploymentName, "deploymentName");
        Objects.requireNonNull(operator, "operator");
        Objects.requireNonNull(topology, "topology");
        Objects.requireNonNull(mongoVersion, "mongoVersion");
        Objects.requireNonNull(auth, "auth");
        Objects.requireNonNull(tls, "tls");
        Objects.requireNonNull(storage, "storage");
        Objects.requireNonNull(resources, "resources");
        Objects.requireNonNull(scheduling, "scheduling");
        Objects.requireNonNull(monitoring, "monitoring");
        Objects.requireNonNull(backup, "backup");
        Objects.requireNonNull(advancedYaml, "advancedYaml");
        if (computeStrategy == null) computeStrategy = ComputeStrategy.NONE;
    }

    /**
     * A sensible starting point for the wizard — Dev/Test profile,
     * MCO, RS3, no TLS. The user walks the steps from here.
     */
    public static ProvisionModel defaults(long clusterId, String namespace,
                                            String deploymentName) {
        return new ProvisionModel(
                Profile.DEV_TEST,
                clusterId,
                namespace,
                false,
                deploymentName,
                OperatorId.MCO,
                Topology.RS3,
                "7.0",
                AuthSpec.defaults(),
                TlsSpec.off(),
                StorageSpec.devDefaults(),
                ResourceSpec.devDefaults(),
                SchedulingSpec.devDefaults(),
                MonitoringSpec.devDefaults(),
                BackupSpec.none(),
                Optional.empty(),
                ComputeStrategy.NONE);
    }

    /* =========================== wither helpers =========================== */

    public ProvisionModel withProfile(Profile v) {
        return new ProvisionModel(v, clusterId, namespace, createNamespace, deploymentName,
                operator, topology, mongoVersion, auth, tls, storage, resources,
                scheduling, monitoring, backup, advancedYaml, computeStrategy);
    }

    public ProvisionModel withOperator(OperatorId v) {
        return new ProvisionModel(profile, clusterId, namespace, createNamespace, deploymentName,
                v, topology, mongoVersion, auth, tls, storage, resources,
                scheduling, monitoring, backup, advancedYaml, computeStrategy);
    }

    public ProvisionModel withTopology(Topology v) {
        return new ProvisionModel(profile, clusterId, namespace, createNamespace, deploymentName,
                operator, v, mongoVersion, auth, tls, storage, resources,
                scheduling, monitoring, backup, advancedYaml, computeStrategy);
    }

    public ProvisionModel withAuth(AuthSpec v) {
        return new ProvisionModel(profile, clusterId, namespace, createNamespace, deploymentName,
                operator, topology, mongoVersion, v, tls, storage, resources,
                scheduling, monitoring, backup, advancedYaml, computeStrategy);
    }

    public ProvisionModel withTls(TlsSpec v) {
        return new ProvisionModel(profile, clusterId, namespace, createNamespace, deploymentName,
                operator, topology, mongoVersion, auth, v, storage, resources,
                scheduling, monitoring, backup, advancedYaml, computeStrategy);
    }

    public ProvisionModel withStorage(StorageSpec v) {
        return new ProvisionModel(profile, clusterId, namespace, createNamespace, deploymentName,
                operator, topology, mongoVersion, auth, tls, v, resources,
                scheduling, monitoring, backup, advancedYaml, computeStrategy);
    }

    public ProvisionModel withResources(ResourceSpec v) {
        return new ProvisionModel(profile, clusterId, namespace, createNamespace, deploymentName,
                operator, topology, mongoVersion, auth, tls, storage, v,
                scheduling, monitoring, backup, advancedYaml, computeStrategy);
    }

    public ProvisionModel withScheduling(SchedulingSpec v) {
        return new ProvisionModel(profile, clusterId, namespace, createNamespace, deploymentName,
                operator, topology, mongoVersion, auth, tls, storage, resources,
                v, monitoring, backup, advancedYaml, computeStrategy);
    }

    public ProvisionModel withMonitoring(MonitoringSpec v) {
        return new ProvisionModel(profile, clusterId, namespace, createNamespace, deploymentName,
                operator, topology, mongoVersion, auth, tls, storage, resources,
                scheduling, v, backup, advancedYaml, computeStrategy);
    }

    public ProvisionModel withBackup(BackupSpec v) {
        return new ProvisionModel(profile, clusterId, namespace, createNamespace, deploymentName,
                operator, topology, mongoVersion, auth, tls, storage, resources,
                scheduling, monitoring, v, advancedYaml, computeStrategy);
    }

    public ProvisionModel withComputeStrategy(ComputeStrategy v) {
        return new ProvisionModel(profile, clusterId, namespace, createNamespace, deploymentName,
                operator, topology, mongoVersion, auth, tls, storage, resources,
                scheduling, monitoring, backup, advancedYaml, v);
    }

    public ProvisionModel withNamespace(String ns, boolean create) {
        return new ProvisionModel(profile, clusterId, ns, create, deploymentName,
                operator, topology, mongoVersion, auth, tls, storage, resources,
                scheduling, monitoring, backup, advancedYaml, computeStrategy);
    }

    public ProvisionModel withDeploymentName(String name) {
        return new ProvisionModel(profile, clusterId, namespace, createNamespace, name,
                operator, topology, mongoVersion, auth, tls, storage, resources,
                scheduling, monitoring, backup, advancedYaml, computeStrategy);
    }

    public ProvisionModel withMongoVersion(String v) {
        return new ProvisionModel(profile, clusterId, namespace, createNamespace, deploymentName,
                operator, topology, v, auth, tls, storage, resources,
                scheduling, monitoring, backup, advancedYaml, computeStrategy);
    }
}
