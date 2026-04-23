package com.kubrik.mex.k8s.operator.mco;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.kubrik.mex.k8s.operator.KubernetesManifests;
import com.kubrik.mex.k8s.provision.AuthSpec;
import com.kubrik.mex.k8s.provision.BackupSpec;
import com.kubrik.mex.k8s.provision.MonitoringSpec;
import com.kubrik.mex.k8s.provision.OperatorId;
import com.kubrik.mex.k8s.provision.ProfileEnforcer;
import com.kubrik.mex.k8s.provision.Profile;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.k8s.provision.SchedulingSpec;
import com.kubrik.mex.k8s.provision.StorageSpec;
import com.kubrik.mex.k8s.provision.TlsSpec;
import com.kubrik.mex.k8s.provision.Topology;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class McoCRRendererTest {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private final McoCRRenderer renderer = new McoCRRenderer();

    @Test
    void dev_rs3_yields_cr_and_no_auxiliary_docs() throws IOException {
        ProvisionModel m = ProvisionModel.defaults(1L, "mongo", "dev-rs");
        KubernetesManifests out = renderer.render(m);

        assertEquals("MongoDBCommunity", out.crKind());
        assertEquals("dev-rs", out.crName());
        assertEquals(1, out.size(), "Dev defaults: CR only, no PDB / SM / PBM");

        Map<?, ?> cr = parse(out.crYaml());
        assertEquals("mongodbcommunity.mongodb.com/v1", cr.get("apiVersion"));
        Map<?, ?> spec = (Map<?, ?>) cr.get("spec");
        assertEquals(3, spec.get("members"));
        assertEquals("7.0", spec.get("version"));
        Map<?, ?> security = (Map<?, ?>) spec.get("security");
        Map<?, ?> tls = (Map<?, ?>) security.get("tls");
        assertEquals(false, tls.get("enabled"));
    }

    @Test
    void prod_rs5_with_cert_manager_renders_tls_secret_refs() throws IOException {
        ProvisionModel m = new ProfileEnforcer().switchProfile(
                ProvisionModel.defaults(1L, "mongo", "prod-rs")
                        .withTopology(Topology.RS5),
                Profile.PROD).model()
                .withTls(TlsSpec.certManager("prod-issuer"))
                .withStorage(new StorageSpec(Optional.of("gp3"), 200, 20));

        KubernetesManifests out = renderer.render(m);

        Map<?, ?> cr = parse(out.crYaml());
        Map<?, ?> spec = (Map<?, ?>) cr.get("spec");
        assertEquals(5, spec.get("members"));

        Map<?, ?> tls = (Map<?, ?>) ((Map<?, ?>) spec.get("security")).get("tls");
        assertEquals(true, tls.get("enabled"));
        Map<?, ?> certRef = (Map<?, ?>) tls.get("certificateKeySecretRef");
        assertEquals("prod-rs-cert", certRef.get("name"));
        Map<?, ?> caRef = (Map<?, ?>) tls.get("caCertificateSecretRef");
        assertEquals("prod-rs-ca", caRef.get("name"));
    }

    @Test
    void prod_emits_pdb_and_service_monitor_and_pbm_bundle() {
        ProvisionModel m = new ProfileEnforcer().switchProfile(
                ProvisionModel.defaults(1L, "mongo", "prod"),
                Profile.PROD).model()
                .withTls(TlsSpec.certManager("prod-issuer"))
                .withStorage(new StorageSpec(Optional.of("gp3"), 200, 20));

        KubernetesManifests out = renderer.render(m);

        List<String> kinds = out.documents().stream()
                .map(KubernetesManifests.Manifest::kind).toList();
        assertTrue(kinds.contains("PodDisruptionBudget"));
        assertTrue(kinds.contains("ServiceMonitor"));
        assertTrue(kinds.contains("ConfigMap"), "PBM ConfigMap");
        assertTrue(kinds.contains("CronJob"), "PBM daily CronJob");
    }

    @Test
    void byo_secret_tls_emits_placeholder_secret() throws IOException {
        ProvisionModel m = new ProfileEnforcer().switchProfile(
                ProvisionModel.defaults(1L, "mongo", "byo"),
                Profile.PROD).model()
                .withTls(TlsSpec.byoSecret("my-existing-tls"))
                .withStorage(new StorageSpec(Optional.of("gp3"), 100, 10));

        KubernetesManifests out = renderer.render(m);

        KubernetesManifests.Manifest tlsSecret = out.documents().stream()
                .filter(d -> d.kind().equals("Secret") && d.name().equals("my-existing-tls"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected BYO TLS Secret placeholder"));
        Map<?, ?> parsed = parse(tlsSecret.yaml());
        assertEquals("kubernetes.io/tls", parsed.get("type"));
    }

    @Test
    void provided_password_emits_secret_with_string_data() throws IOException {
        ProvisionModel m = ProvisionModel.defaults(1L, "mongo", "dev")
                .withAuth(new AuthSpec("mongoAdmin",
                        AuthSpec.PasswordMode.PROVIDE, Optional.of("s3cret!")));
        KubernetesManifests out = renderer.render(m);

        KubernetesManifests.Manifest sec = out.documents().stream()
                .filter(d -> d.kind().equals("Secret")
                        && d.name().equals("dev-admin-user"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected password Secret"));
        Map<?, ?> parsed = parse(sec.yaml());
        Map<?, ?> stringData = (Map<?, ?>) parsed.get("stringData");
        assertEquals("s3cret!", stringData.get("password"));
    }

    @Test
    void scram_user_and_admin_db_stamped_on_cr() throws IOException {
        ProvisionModel m = ProvisionModel.defaults(1L, "mongo", "dev");
        Map<?, ?> cr = parse(renderer.render(m).crYaml());
        Map<?, ?> spec = (Map<?, ?>) cr.get("spec");
        List<?> users = (List<?>) spec.get("users");
        assertEquals(1, users.size());
        Map<?, ?> user = (Map<?, ?>) users.get(0);
        assertEquals("mongoAdmin", user.get("name"));
        assertEquals("admin", user.get("db"));
        Map<?, ?> passRef = (Map<?, ?>) user.get("passwordSecretRef");
        assertEquals("dev-admin-user", passRef.get("name"));
    }

    @Test
    void render_is_deterministic() {
        ProvisionModel m = ProvisionModel.defaults(1L, "mongo", "dev");
        assertEquals(renderer.render(m).crYaml(), renderer.render(m).crYaml(),
                "render must be pure — same input, same bytes (PreviewHashChecker depends on this)");
    }

    @Test
    void cr_labels_include_mex_renderer_marker() throws IOException {
        ProvisionModel m = ProvisionModel.defaults(1L, "mongo", "dev");
        Map<?, ?> cr = parse(renderer.render(m).crYaml());
        Map<?, ?> meta = (Map<?, ?>) cr.get("metadata");
        Map<?, ?> labels = (Map<?, ?>) meta.get("labels");
        assertEquals(McoCRRenderer.MEX_LABEL_VALUE, labels.get(McoCRRenderer.MEX_LABEL));
    }

    @Test
    void dev_profile_does_not_render_pbm_bundle() {
        ProvisionModel m = ProvisionModel.defaults(1L, "mongo", "dev");
        // Verify default backup is NONE (dev defaults) and no PBM docs.
        assertEquals(BackupSpec.Mode.NONE, m.backup().mode());
        List<String> kinds = renderer.render(m).documents().stream()
                .map(KubernetesManifests.Manifest::kind).toList();
        assertFalse(kinds.contains("CronJob"));
    }

    private static Map<?, ?> parse(String yaml) throws IOException {
        return YAML.readValue(yaml, Map.class);
    }
}
