package com.kubrik.mex.k8s.compute.managedpool;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.container.v1.CreateNodePoolRequest;
import com.google.container.v1.NodePool;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.4.1 — Locks the wire shape the GKE adapter sends to
 * {@code container.nodePools.create} without standing up a live
 * project. Each test exercises {@link GkeAdapter#buildCreateRequest}
 * directly and asserts the resulting proto matches the user's spec.
 */
class GkeAdapterTest {

    @Test
    void on_demand_spec_renders_node_pool_proto_with_machine_type_and_autoscale_bounds() {
        ManagedPoolSpec spec = ManagedPoolSpec.sensibleEksDefaults(
                7L, "us-central1", "prod-rs");
        // Override capacity to ON_DEMAND for clarity (defaults already
        // are, but assert the test case is explicit).
        ManagedPoolSpec onDemand = new ManagedPoolSpec(
                CloudProvider.GCP, 7L, "us-central1", "mex-prod-rs",
                "n2-standard-4", ManagedPoolSpec.CapacityType.ON_DEMAND,
                3, 3, 5, "amd64", List.of(), List.of());

        CreateNodePoolRequest req = GkeAdapter.buildCreateRequest(
                onDemand, "projects/p/locations/us-central1/clusters/c");

        assertEquals("projects/p/locations/us-central1/clusters/c", req.getParent());
        NodePool pool = req.getNodePool();
        assertEquals("mex-prod-rs", pool.getName());
        assertEquals(3, pool.getInitialNodeCount());

        assertTrue(pool.getAutoscaling().getEnabled());
        assertEquals(3, pool.getAutoscaling().getMinNodeCount());
        assertEquals(5, pool.getAutoscaling().getMaxNodeCount());

        assertEquals("n2-standard-4", pool.getConfig().getMachineType());
        assertFalse(pool.getConfig().getSpot(), "ON_DEMAND must not set spot=true");
    }

    @Test
    void spot_capacity_translates_to_node_config_spot_true() {
        ManagedPoolSpec spec = new ManagedPoolSpec(
                CloudProvider.GCP, 1L, "europe-west1", "mex-spot-rs",
                "e2-standard-2", ManagedPoolSpec.CapacityType.SPOT,
                1, 2, 4, "amd64", List.of(), List.of());

        CreateNodePoolRequest req = GkeAdapter.buildCreateRequest(spec, "p");
        assertTrue(req.getNodePool().getConfig().getSpot(),
                "SPOT capacity must lift to NodeConfig.spot=true on the wire");
    }

    @Test
    void service_account_key_path_resolves_to_fixed_credentials_provider() throws Exception {
        // Generate a real RSA key on the fly so Google's SDK can
        // PKCS-8 decode it. Static fixture keys break linters /
        // secret scanners; an ephemeral key avoids that whole class
        // of footgun.
        java.security.KeyPairGenerator gen =
                java.security.KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        java.security.KeyPair kp = gen.generateKeyPair();
        String pemBody = java.util.Base64.getMimeEncoder(
                        64, new byte[]{'\n'})
                .encodeToString(kp.getPrivate().getEncoded());
        String pem = "-----BEGIN PRIVATE KEY-----\n" + pemBody
                + "\n-----END PRIVATE KEY-----\n";
        String pemEscaped = pem.replace("\n", "\\n");
        String sa = "{\"type\":\"service_account\","
                + "\"project_id\":\"p\","
                + "\"private_key_id\":\"k\","
                + "\"private_key\":\"" + pemEscaped + "\","
                + "\"client_email\":\"x@p.iam.gserviceaccount.com\","
                + "\"client_id\":\"1\","
                + "\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\","
                + "\"token_uri\":\"https://oauth2.googleapis.com/token\"}";
        InMemorySecretStore store = new InMemorySecretStore();
        String ref = SecretStore.newRef(CloudProvider.GCP, "test");
        store.store(ref, sa);

        CloudCredential cred = new CloudCredential(1L, "test-gke", CloudProvider.GCP,
                CloudCredential.AuthMode.SERVICE_ACCOUNT_KEY, ref,
                Optional.empty(), Optional.of("p"),
                Optional.empty(), Optional.of("us-central1"),
                0L, Optional.empty(), Optional.empty());

        var prov = new GkeAdapter(store).providerFor(cred);
        assertInstanceOf(FixedCredentialsProvider.class, prov,
                "SERVICE_ACCOUNT_KEY with a valid payload must yield a "
                + "FixedCredentialsProvider scoped to cloud-platform");
    }

    @Test
    void empty_service_account_payload_falls_back_to_default_chain() throws Exception {
        InMemorySecretStore store = new InMemorySecretStore();
        String ref = SecretStore.newRef(CloudProvider.GCP, "empty");
        // Don't store anything — store.read returns Optional.empty().
        CloudCredential cred = new CloudCredential(1L, "empty-gke", CloudProvider.GCP,
                CloudCredential.AuthMode.SERVICE_ACCOUNT_KEY, ref,
                Optional.empty(), Optional.of("p"),
                Optional.empty(), Optional.empty(),
                0L, Optional.empty(), Optional.empty());

        var prov = new GkeAdapter(store).providerFor(cred);
        assertInstanceOf(GoogleCredentialsProvider.class, prov,
                "Empty payload must fall back to ADC instead of throwing");
    }

    @Test
    void workload_identity_uses_default_chain() throws Exception {
        CloudCredential cred = new CloudCredential(1L, "wi", CloudProvider.GCP,
                CloudCredential.AuthMode.WORKLOAD_IDENTITY, "keychain://wi",
                Optional.empty(), Optional.of("p"),
                Optional.empty(), Optional.empty(),
                0L, Optional.empty(), Optional.empty());

        var prov = new GkeAdapter(new InMemorySecretStore()).providerFor(cred);
        assertInstanceOf(GoogleCredentialsProvider.class, prov);
    }
}
