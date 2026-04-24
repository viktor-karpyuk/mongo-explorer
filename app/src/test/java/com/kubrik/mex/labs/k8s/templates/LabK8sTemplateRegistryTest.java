package com.kubrik.mex.labs.k8s.templates;

import com.kubrik.mex.k8s.provision.BackupSpec;
import com.kubrik.mex.k8s.provision.OperatorId;
import com.kubrik.mex.k8s.provision.Profile;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.k8s.provision.TlsSpec;
import com.kubrik.mex.k8s.provision.Topology;
import com.kubrik.mex.labs.k8s.model.LabK8sDistro;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LabK8sTemplateRegistryTest {

    private final LabK8sTemplateRegistry reg = new LabK8sTemplateRegistry();

    @Test
    void five_day_one_templates_registered_in_order() {
        assertEquals(java.util.List.of(
                        "psmdb-rs3", "mco-rs3", "psmdb-sharded",
                        "psmdb-pbm-backup", "mco-tls-keyfile"),
                reg.ids());
    }

    @Test
    void psmdb_rs3_renders_with_expected_dev_shape() {
        ProvisionModel m = reg.byIdOrThrow("psmdb-rs3")
                .factory().build(42L, "mongo", "dev-rs");
        assertEquals(OperatorId.PSMDB, m.operator());
        assertEquals(Topology.RS3, m.topology());
        assertEquals(Profile.DEV_TEST, m.profile());
        assertEquals(TlsSpec.Mode.OFF, m.tls().mode());
        assertEquals(BackupSpec.Mode.NONE, m.backup().mode());
    }

    @Test
    void mco_rs3_uses_mco_and_rs3() {
        ProvisionModel m = reg.byIdOrThrow("mco-rs3")
                .factory().build(1L, "mongo", "mco-rs");
        assertEquals(OperatorId.MCO, m.operator());
        assertEquals(Topology.RS3, m.topology());
    }

    @Test
    void psmdb_sharded_is_prod_plus_sharded_plus_cert_manager() {
        ProvisionModel m = reg.byIdOrThrow("psmdb-sharded")
                .factory().build(1L, "mongo", "shards");
        assertEquals(Topology.SHARDED, m.topology());
        assertEquals(Profile.PROD, m.profile());
        assertEquals(TlsSpec.Mode.CERT_MANAGER, m.tls().mode());
        assertTrue(reg.byIdOrThrow("psmdb-sharded")
                .tags().contains("requires:cert-manager"));
    }

    @Test
    void psmdb_pbm_backup_enables_native_pbm() {
        ProvisionModel m = reg.byIdOrThrow("psmdb-pbm-backup")
                .factory().build(1L, "mongo", "rs5");
        assertEquals(BackupSpec.Mode.PSMDB_PBM, m.backup().mode());
        assertEquals(Topology.RS5, m.topology());
    }

    @Test
    void mco_tls_keyfile_uses_operator_generated_tls() {
        ProvisionModel m = reg.byIdOrThrow("mco-tls-keyfile")
                .factory().build(1L, "mongo", "tls-rs");
        assertEquals(TlsSpec.Mode.OPERATOR_GENERATED, m.tls().mode());
    }

    @Test
    void every_template_supports_both_distros() {
        for (LabK8sTemplate t : reg.all()) {
            assertTrue(t.supportedDistros().contains(LabK8sDistro.MINIKUBE),
                    t.id() + " must support minikube");
            assertTrue(t.supportedDistros().contains(LabK8sDistro.K3D),
                    t.id() + " must support k3d");
        }
    }

    @Test
    void unknown_id_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> reg.byIdOrThrow("does-not-exist"));
    }

    @Test
    void factory_is_deterministic_across_calls() {
        LabK8sTemplate t = reg.byIdOrThrow("psmdb-rs3");
        ProvisionModel a = t.factory().build(1L, "mongo", "rs");
        ProvisionModel b = t.factory().build(1L, "mongo", "rs");
        assertEquals(a, b);
    }
}
