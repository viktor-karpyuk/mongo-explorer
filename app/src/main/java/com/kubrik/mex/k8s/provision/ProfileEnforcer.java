package com.kubrik.mex.k8s.provision;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-D2 — Pure-function rule engine consulted on every
 * wizard field edit.
 *
 * <p>Two roles:</p>
 * <ol>
 *   <li>{@link #verdict(ProvisionModel, String)} answers "what
 *       should this field do?" for a given field id. The wizard
 *       uses the answer to render the control read-only, mark it
 *       required with a star, pre-fill a default, and surface the
 *       rationale as a tooltip.</li>
 *   <li>{@link #switchProfile(ProvisionModel, Profile)} routes a
 *       profile switch through the full rule set, applying every
 *       Prod-required default and returning the change log so the
 *       UI can acknowledge them in a single dialog
 *       (spec §5).</li>
 * </ol>
 *
 * <p>Rules are hard-coded for Alpha. When the blessed matrix
 * widens (milestone §9.2) we'll externalise to the
 * {@code profile-rules.yaml} the tech-spec mentions; keeping the
 * first cut inline makes the rules reviewable in code review
 * without a side-file.</p>
 */
public final class ProfileEnforcer {

    /* Field ids — the wizard passes these into verdict(). Keeping
     * them as constants rather than an enum so the future YAML path
     * can emit new ids without a code change. */
    public static final String F_TLS_MODE       = "tls.mode";
    public static final String F_STORAGE_CLASS  = "storage.class";
    public static final String F_STORAGE_SIZE   = "storage.dataSizeGib";
    public static final String F_DATA_CPU_REQ   = "resources.dataCpuRequest";
    public static final String F_DATA_MEM_REQ   = "resources.dataMemRequest";
    public static final String F_PDB            = "scheduling.pdb";
    public static final String F_TOPOLOGY_SPREAD = "scheduling.topologySpread";
    public static final String F_SERVICE_MONITOR = "monitoring.serviceMonitor";
    public static final String F_BACKUP_MODE    = "backup.mode";
    public static final String F_DELETION_PROT  = "deletionProtection";
    public static final String F_MONGO_VERSION  = "mongoVersion";
    public static final String F_DEPLOYMENT_NAME = "deploymentName";

    public FieldVerdict verdict(ProvisionModel m, String fieldId) {
        return switch (m.profile()) {
            case DEV_TEST -> devVerdict(m, fieldId);
            case PROD     -> prodVerdict(m, fieldId);
        };
    }

    private FieldVerdict devVerdict(ProvisionModel m, String fieldId) {
        // Dev/Test is a preset: every field is editable. A couple
        // are required to make the CR render at all.
        return switch (fieldId) {
            case F_DEPLOYMENT_NAME, F_MONGO_VERSION ->
                    FieldVerdict.requiredWith(null, "Required for the operator to render a CR.");
            default -> FieldVerdict.optional();
        };
    }

    private FieldVerdict prodVerdict(ProvisionModel m, String fieldId) {
        return switch (fieldId) {
            case F_DEPLOYMENT_NAME, F_MONGO_VERSION ->
                    FieldVerdict.requiredWith(null, "Required for the operator to render a CR.");

            case F_TLS_MODE ->
                    FieldVerdict.requiredWith(TlsSpec.Mode.CERT_MANAGER,
                            "Prod enforces encryption at rest and in flight. "
                            + "Pick cert-manager for managed issuance or "
                            + "BYO-Secret for an existing CA.");

            case F_STORAGE_SIZE ->
                    FieldVerdict.requiredWith(StorageSpec.prodDefaults().dataSizeGib(),
                            "Prod deployments need an explicit storage commitment. "
                            + "Default is " + StorageSpec.prodDefaults().dataSizeGib() + " GiB per data pod.");

            case F_STORAGE_CLASS ->
                    FieldVerdict.requiredWith(null,
                            "Pick a storage class so the PVC binding target is "
                            + "explicit — the cluster default may not survive a node-pool rotation.");

            case F_DATA_CPU_REQ, F_DATA_MEM_REQ ->
                    FieldVerdict.requiredWith(null,
                            "Prod requires explicit CPU+memory requests so the scheduler "
                            + "can reliably place pods under load. Limits optional.");

            case F_PDB ->
                    FieldVerdict.locked(true,
                            "A PodDisruptionBudget prevents involuntary eviction of "
                            + "majority members during a node drain. Locked on in Prod.");

            case F_TOPOLOGY_SPREAD ->
                    FieldVerdict.locked(true,
                            "TopologySpreadConstraints across zones/nodes prevent a single-"
                            + "zone outage from taking down the replset. Locked on in Prod.");

            case F_SERVICE_MONITOR ->
                    FieldVerdict.locked(true,
                            "Prod deployments ship with Prometheus scrape on so the v2.1 "
                            + "monitoring surfaces have data from the first minute.");

            case F_BACKUP_MODE ->
                    FieldVerdict.requiredWith(defaultBackupFor(m.operator()),
                            "A Prod deployment never ships without a declared backup "
                            + "strategy. Pick the operator-native path, a managed CronJob, "
                            + "or check the BYO-declared box if you have an out-of-band plan.");

            case F_DELETION_PROT ->
                    FieldVerdict.locked(true,
                            "Deletion protection is on by default in Prod. Users turn it "
                            + "off explicitly before a Delete action.");

            default -> FieldVerdict.optional();
        };
    }

    private static BackupSpec.Mode defaultBackupFor(OperatorId op) {
        return switch (op) {
            case PSMDB -> BackupSpec.Mode.PSMDB_PBM;
            case MCO   -> BackupSpec.Mode.MANAGED_PBM_CRONJOB;
        };
    }

    /**
     * Route a profile switch through every Prod-required default.
     *
     * @return the new model plus a list of human-readable changes
     *   the UI should surface in the acknowledge dialog.
     */
    public SwitchResult switchProfile(ProvisionModel m, Profile to) {
        if (m.profile() == to) return new SwitchResult(m, List.of());
        ProvisionModel next = m.withProfile(to);
        List<String> changes = new ArrayList<>();
        if (to == Profile.PROD) {
            next = applyProdDefaults(m, next, changes);
        } else {
            // DEV_TEST: don't forcibly overwrite prod settings the user
            // may want to keep — just relax the locks. The UI re-
            // renders with an optional verdict on those fields.
            changes.add("Prod guardrails relaxed — all fields are editable now.");
        }
        return new SwitchResult(next, changes);
    }

    private ProvisionModel applyProdDefaults(ProvisionModel before, ProvisionModel after,
                                               List<String> changes) {
        if (!before.tls().isProdAcceptable()) {
            after = after.withTls(TlsSpec.certManager("mongo-issuer"));
            changes.add("TLS switched to cert-manager (issuer: mongo-issuer).");
        }
        if (!before.scheduling().pdbEnabled() || !before.scheduling().topologySpread()) {
            after = after.withScheduling(SchedulingSpec.prodDefaults());
            changes.add("PDB + TopologySpread locked on for Prod.");
        }
        if (!before.monitoring().serviceMonitor()) {
            after = after.withMonitoring(MonitoringSpec.prodDefaults());
            changes.add("ServiceMonitor locked on for Prod.");
        }
        if (!before.resources().hasDataRequests()) {
            after = after.withResources(ResourceSpec.prodDefaults());
            changes.add("Data pod CPU+memory requests defaulted to Prod minimums.");
        }
        if (before.storage().dataSizeGib() < StorageSpec.prodDefaults().dataSizeGib()) {
            after = after.withStorage(new StorageSpec(
                    before.storage().storageClass(),
                    StorageSpec.prodDefaults().dataSizeGib(),
                    Math.max(before.storage().configServerSizeGib(),
                            StorageSpec.prodDefaults().configServerSizeGib())));
            changes.add("Storage size raised to " + StorageSpec.prodDefaults().dataSizeGib()
                    + " GiB per data pod.");
        }
        if (!before.backup().isProdAcceptable()) {
            after = after.withBackup(new BackupSpec(defaultBackupFor(after.operator())));
            changes.add("Backup set to " + defaultBackupFor(after.operator()) + ".");
        }
        // Prod topology availability narrows — if the user picked
        // STANDALONE on Dev, bump to RS3.
        if (!TopologyPicker.availableTopologies(Profile.PROD, after.operator())
                .contains(after.topology())) {
            after = after.withTopology(Topology.RS3);
            changes.add("Topology bumped to RS3 — standalone isn't supported in Prod.");
        }
        return after;
    }

    /**
     * Apply-time validation. Returns blocking issues — the Apply
     * button is disabled when any is present. Pre-flight (Q2.8.1-G)
     * layers further checks that need live cluster state.
     */
    public List<String> validate(ProvisionModel m) {
        List<String> issues = new ArrayList<>();
        if (m.deploymentName().isBlank()) issues.add("Deployment name is required.");
        if (m.namespace().isBlank()) issues.add("Namespace is required.");
        if (m.mongoVersion().isBlank()) issues.add("Mongo version is required.");
        if (!TopologyPicker.availableTopologies(m.profile(), m.operator())
                .contains(m.topology())) {
            issues.add("Topology " + m.topology() + " is not available for "
                    + m.profile() + " + " + m.operator() + ".");
        }
        if (m.profile() == Profile.PROD) {
            if (!m.tls().isProdAcceptable()) {
                issues.add("Prod requires TLS (cert-manager or BYO-Secret).");
            }
            if (!m.resources().hasDataRequests()) {
                issues.add("Prod requires explicit data-pod CPU+memory requests.");
            }
            if (!m.backup().isProdAcceptable()) {
                issues.add("Prod requires a declared backup strategy.");
            }
            if (!m.scheduling().pdbEnabled() || !m.scheduling().topologySpread()) {
                issues.add("Prod requires PDB + TopologySpread on.");
            }
            if (m.storage().dataSizeGib() < 1) {
                issues.add("Prod requires a positive data storage size.");
            }
        }
        return issues;
    }

    public record SwitchResult(ProvisionModel model, List<String> changes) {}

    /** Map form of the Prod defaults — handy for tests + docs. */
    public static Map<String, Object> prodLockedValues() {
        return Map.of(
                F_PDB, true,
                F_TOPOLOGY_SPREAD, true,
                F_SERVICE_MONITOR, true,
                F_DELETION_PROT, true);
    }
}
