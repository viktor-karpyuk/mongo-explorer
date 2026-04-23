package com.kubrik.mex.k8s.provision;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProfileEnforcerTest {

    private final ProfileEnforcer enforcer = new ProfileEnforcer();

    @Test
    void dev_test_leaves_most_fields_optional() {
        ProvisionModel m = defaults();
        assertFalse(enforcer.verdict(m, ProfileEnforcer.F_TLS_MODE).required());
        assertFalse(enforcer.verdict(m, ProfileEnforcer.F_PDB).isLocked());
        assertFalse(enforcer.verdict(m, ProfileEnforcer.F_BACKUP_MODE).required());
    }

    @Test
    void dev_test_still_requires_name_and_version() {
        ProvisionModel m = defaults();
        assertTrue(enforcer.verdict(m, ProfileEnforcer.F_DEPLOYMENT_NAME).required());
        assertTrue(enforcer.verdict(m, ProfileEnforcer.F_MONGO_VERSION).required());
    }

    @Test
    void prod_locks_pdb_topology_monitor_deletion_protection() {
        ProvisionModel m = defaults().withProfile(Profile.PROD);
        assertTrue(enforcer.verdict(m, ProfileEnforcer.F_PDB).isLocked());
        assertTrue(enforcer.verdict(m, ProfileEnforcer.F_TOPOLOGY_SPREAD).isLocked());
        assertTrue(enforcer.verdict(m, ProfileEnforcer.F_SERVICE_MONITOR).isLocked());
        assertTrue(enforcer.verdict(m, ProfileEnforcer.F_DELETION_PROT).isLocked());
    }

    @Test
    void prod_requires_tls_storage_size_requests_backup() {
        ProvisionModel m = defaults().withProfile(Profile.PROD);
        assertTrue(enforcer.verdict(m, ProfileEnforcer.F_TLS_MODE).required());
        assertFalse(enforcer.verdict(m, ProfileEnforcer.F_TLS_MODE).isLocked(),
                "Prod requires TLS but user picks cert-manager or BYO-Secret");
        assertTrue(enforcer.verdict(m, ProfileEnforcer.F_STORAGE_SIZE).required());
        assertTrue(enforcer.verdict(m, ProfileEnforcer.F_DATA_CPU_REQ).required());
        assertTrue(enforcer.verdict(m, ProfileEnforcer.F_BACKUP_MODE).required());
    }

    @Test
    void prod_backup_default_depends_on_operator() {
        ProvisionModel mco = defaults().withProfile(Profile.PROD).withOperator(OperatorId.MCO);
        assertEquals(BackupSpec.Mode.MANAGED_PBM_CRONJOB,
                enforcer.verdict(mco, ProfileEnforcer.F_BACKUP_MODE).defaultValue().orElseThrow());

        ProvisionModel psmdb = mco.withOperator(OperatorId.PSMDB);
        assertEquals(BackupSpec.Mode.PSMDB_PBM,
                enforcer.verdict(psmdb, ProfileEnforcer.F_BACKUP_MODE).defaultValue().orElseThrow());
    }

    @Test
    void switch_to_prod_applies_every_default_and_logs_changes() {
        ProvisionModel dev = defaults();
        ProfileEnforcer.SwitchResult r = enforcer.switchProfile(dev, Profile.PROD);
        assertEquals(Profile.PROD, r.model().profile());
        assertTrue(r.model().tls().isProdAcceptable());
        assertTrue(r.model().scheduling().pdbEnabled());
        assertTrue(r.model().monitoring().serviceMonitor());
        assertTrue(r.model().resources().hasDataRequests());
        assertEquals(BackupSpec.Mode.MANAGED_PBM_CRONJOB, r.model().backup().mode());
        assertFalse(r.changes().isEmpty(), "changes log must list every lock/default applied");
    }

    @Test
    void switch_to_prod_bumps_standalone_to_rs3() {
        ProvisionModel dev = defaults().withTopology(Topology.STANDALONE);
        ProfileEnforcer.SwitchResult r = enforcer.switchProfile(dev, Profile.PROD);
        assertEquals(Topology.RS3, r.model().topology());
    }

    @Test
    void switch_prod_to_dev_relaxes_but_doesnt_wipe() {
        ProvisionModel prod = enforcer.switchProfile(defaults(), Profile.PROD).model();
        ProfileEnforcer.SwitchResult back = enforcer.switchProfile(prod, Profile.DEV_TEST);
        assertEquals(Profile.DEV_TEST, back.model().profile());
        assertTrue(back.model().tls().isProdAcceptable(),
                "prod-picked TLS mode should survive going back to dev");
        assertTrue(back.model().scheduling().pdbEnabled(),
                "prod-picked scheduling should survive going back to dev");
        assertEquals(1, back.changes().size());
    }

    @Test
    void switch_profile_to_same_is_no_op() {
        ProvisionModel m = defaults();
        ProfileEnforcer.SwitchResult r = enforcer.switchProfile(m, Profile.DEV_TEST);
        assertSame(m, r.model());
        assertTrue(r.changes().isEmpty());
    }

    @Test
    void validate_flags_empty_required_fields() {
        ProvisionModel bad = defaults().withDeploymentName("");
        List<String> issues = enforcer.validate(bad);
        assertTrue(issues.stream().anyMatch(s -> s.contains("Deployment name")));
    }

    @Test
    void validate_blocks_prod_without_tls_or_backup_or_resources() {
        ProvisionModel m = defaults().withProfile(Profile.PROD);
        // Still has Dev/Test defaults in place — so validate() should
        // return every prod-required issue.
        List<String> issues = enforcer.validate(m);
        assertTrue(issues.stream().anyMatch(s -> s.contains("TLS")));
        assertTrue(issues.stream().anyMatch(s -> s.contains("backup")));
        assertTrue(issues.stream().anyMatch(s -> s.contains("requests")));
    }

    @Test
    void validate_blocks_unavailable_topology_combinations() {
        ProvisionModel bad = defaults()
                .withProfile(Profile.DEV_TEST)
                .withTopology(Topology.SHARDED);
        List<String> issues = enforcer.validate(bad);
        assertTrue(issues.stream().anyMatch(s -> s.contains("SHARDED")));
    }

    @Test
    void validate_prod_fully_configured_is_clean() {
        ProvisionModel m = enforcer.switchProfile(defaults().withOperator(OperatorId.PSMDB),
                Profile.PROD).model()
                .withStorage(new StorageSpec(java.util.Optional.of("gp3"), 200, 20));
        // mongoVersion + deploymentName still "dev" defaults — fine.
        List<String> issues = enforcer.validate(m);
        assertTrue(issues.isEmpty(), "expected clean validation, got: " + issues);
    }

    private static ProvisionModel defaults() {
        return ProvisionModel.defaults(42L, "mongo", "test-rs");
    }
}
