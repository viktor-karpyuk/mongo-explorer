package com.kubrik.mex.k8s.operator.psmdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.kubrik.mex.k8s.operator.KubernetesManifests;
import com.kubrik.mex.k8s.provision.BackupSpec;
import com.kubrik.mex.k8s.provision.OperatorId;
import com.kubrik.mex.k8s.provision.Profile;
import com.kubrik.mex.k8s.provision.ProfileEnforcer;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.k8s.provision.StorageSpec;
import com.kubrik.mex.k8s.provision.TlsSpec;
import com.kubrik.mex.k8s.provision.Topology;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class PsmdbCRRendererTest {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private final PsmdbCRRenderer renderer = new PsmdbCRRenderer();

    @Test
    void dev_rs3_yields_cr_plus_users_secret() throws IOException {
        ProvisionModel m = ProvisionModel.defaults(1L, "mongo", "dev-rs")
                .withOperator(OperatorId.PSMDB);
        KubernetesManifests out = renderer.render(m);

        assertEquals("PerconaServerMongoDB", out.crKind());
        List<String> kinds = out.documents().stream()
                .map(KubernetesManifests.Manifest::kind).toList();
        assertTrue(kinds.contains("Secret"), "users Secret must always emit");
        assertTrue(kinds.contains("PerconaServerMongoDB"));

        Map<?, ?> cr = parse(out.crYaml());
        Map<?, ?> spec = (Map<?, ?>) cr.get("spec");
        List<?> replsets = (List<?>) spec.get("replsets");
        assertEquals(1, replsets.size());
        assertEquals(3, ((Map<?, ?>) replsets.get(0)).get("size"));
    }

    @Test
    void sharded_emits_three_shard_replsets_plus_cfg_plus_mongos() throws IOException {
        ProvisionModel m = new ProfileEnforcer().switchProfile(
                ProvisionModel.defaults(1L, "mongo", "shard")
                        .withOperator(OperatorId.PSMDB)
                        .withTopology(Topology.SHARDED),
                Profile.PROD).model()
                .withTls(TlsSpec.certManager("mongo-issuer"))
                .withStorage(new StorageSpec(Optional.of("gp3"), 200, 20));

        KubernetesManifests out = renderer.render(m);
        Map<?, ?> cr = parse(out.crYaml());
        Map<?, ?> spec = (Map<?, ?>) cr.get("spec");

        List<?> replsets = (List<?>) spec.get("replsets");
        assertEquals(3, replsets.size(), "SHARDED = 3-shard starter (milestone §9.7)");
        for (Object rs : replsets) {
            assertEquals(3, ((Map<?, ?>) rs).get("size"));
        }

        Map<?, ?> sharding = (Map<?, ?>) spec.get("sharding");
        assertEquals(true, sharding.get("enabled"));
        assertNotNull(sharding.get("configsvrReplSet"));
        assertNotNull(sharding.get("mongos"));
    }

    @Test
    void prod_pbm_backup_emits_enabled_with_storages_skeleton() throws IOException {
        ProvisionModel m = new ProfileEnforcer().switchProfile(
                ProvisionModel.defaults(1L, "mongo", "prod")
                        .withOperator(OperatorId.PSMDB),
                Profile.PROD).model()
                .withTls(TlsSpec.certManager("mongo-issuer"))
                .withStorage(new StorageSpec(Optional.of("gp3"), 200, 20));

        Map<?, ?> cr = parse(renderer.render(m).crYaml());
        Map<?, ?> spec = (Map<?, ?>) cr.get("spec");
        Map<?, ?> backup = (Map<?, ?>) spec.get("backup");
        assertEquals(true, backup.get("enabled"));
        Map<?, ?> storages = (Map<?, ?>) backup.get("storages");
        assertTrue(storages.containsKey("default-s3"));
    }

    @Test
    void dev_defaults_backup_disabled() throws IOException {
        ProvisionModel m = ProvisionModel.defaults(1L, "mongo", "dev")
                .withOperator(OperatorId.PSMDB);
        assertEquals(BackupSpec.Mode.NONE, m.backup().mode());
        Map<?, ?> cr = parse(renderer.render(m).crYaml());
        Map<?, ?> backup = (Map<?, ?>) ((Map<?, ?>) cr.get("spec")).get("backup");
        assertEquals(false, backup.get("enabled"));
    }

    @Test
    void tls_mode_cert_manager_wires_issuer_conf() throws IOException {
        ProvisionModel m = ProvisionModel.defaults(1L, "mongo", "dev")
                .withOperator(OperatorId.PSMDB)
                .withTls(TlsSpec.certManager("prod-issuer"));
        Map<?, ?> cr = parse(renderer.render(m).crYaml());
        Map<?, ?> tls = (Map<?, ?>) ((Map<?, ?>) cr.get("spec")).get("tls");
        assertEquals("requireTLS", tls.get("mode"));
        Map<?, ?> issuerConf = (Map<?, ?>) tls.get("issuerConf");
        assertEquals("prod-issuer", issuerConf.get("name"));
        assertEquals("cert-manager.io", issuerConf.get("group"));
    }

    @Test
    void byo_tls_secret_placeholder_emitted() {
        ProvisionModel m = ProvisionModel.defaults(1L, "mongo", "byo")
                .withOperator(OperatorId.PSMDB)
                .withTls(TlsSpec.byoSecret("my-ssl"));

        KubernetesManifests out = renderer.render(m);
        assertTrue(out.documents().stream()
                .anyMatch(d -> d.kind().equals("Secret") && d.name().equals("my-ssl")));
    }

    @Test
    void pmm_enabled_when_service_monitor_enabled() throws IOException {
        ProvisionModel m = new ProfileEnforcer().switchProfile(
                ProvisionModel.defaults(1L, "mongo", "prod")
                        .withOperator(OperatorId.PSMDB),
                Profile.PROD).model()
                .withTls(TlsSpec.certManager("mongo-issuer"))
                .withStorage(new StorageSpec(Optional.of("gp3"), 200, 20));

        Map<?, ?> cr = parse(renderer.render(m).crYaml());
        Map<?, ?> pmm = (Map<?, ?>) ((Map<?, ?>) cr.get("spec")).get("pmm");
        assertEquals(true, pmm.get("enabled"));
    }

    @Test
    void users_secret_has_every_system_user_present() throws IOException {
        ProvisionModel m = ProvisionModel.defaults(1L, "mongo", "dev")
                .withOperator(OperatorId.PSMDB);
        KubernetesManifests.Manifest users = renderer.render(m).documents().stream()
                .filter(d -> d.kind().equals("Secret") && d.name().endsWith("-secrets"))
                .findFirst().orElseThrow();
        Map<?, ?> parsed = parse(users.yaml());
        Map<?, ?> stringData = (Map<?, ?>) parsed.get("stringData");
        assertTrue(stringData.containsKey("MONGODB_CLUSTER_ADMIN_USER"));
        assertTrue(stringData.containsKey("MONGODB_CLUSTER_MONITOR_USER"));
        assertTrue(stringData.containsKey("MONGODB_BACKUP_USER"));
        assertTrue(stringData.containsKey("MONGODB_USER_ADMIN_USER"));
    }

    @Test
    void render_is_deterministic() {
        ProvisionModel m = ProvisionModel.defaults(1L, "mongo", "dev")
                .withOperator(OperatorId.PSMDB);
        assertEquals(renderer.render(m).crYaml(), renderer.render(m).crYaml());
    }

    @Test
    void standalone_emits_unsafe_allowed_and_no_affinity() throws IOException {
        ProvisionModel m = ProvisionModel.defaults(1L, "mongo", "solo")
                .withOperator(OperatorId.PSMDB)
                .withTopology(Topology.STANDALONE);
        Map<?, ?> cr = parse(renderer.render(m).crYaml());
        Map<?, ?> spec = (Map<?, ?>) cr.get("spec");
        assertEquals(true, spec.get("allowUnsafeConfigurations"));
        Map<?, ?> rs = (Map<?, ?>) ((List<?>) spec.get("replsets")).get(0);
        assertFalse(rs.containsKey("affinity"), "STANDALONE shouldn't bind anti-affinity");
    }

    private static Map<?, ?> parse(String yaml) throws IOException {
        return YAML.readValue(yaml, Map.class);
    }
}
