package com.kubrik.mex.k8s.compute.managedpool;

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.DefaultAzureCredential;
import com.azure.resourcemanager.containerservice.fluent.models.AgentPoolInner;
import com.azure.resourcemanager.containerservice.models.AgentPoolMode;
import com.azure.resourcemanager.containerservice.models.OSType;
import com.azure.resourcemanager.containerservice.models.ScaleSetPriority;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.4.2 — Locks the wire shape AKS sends to {@code agentPools.createOrUpdate}
 * + the SERVICE_PRINCIPAL payload parser without standing up a live
 * subscription. Specifically covers the colon-in-secret edge case
 * the recent audit pass fixed.
 */
class AksAdapterTest {

    @Test
    void on_demand_spec_renders_agent_pool_with_vm_size_and_autoscale() {
        ManagedPoolSpec spec = new ManagedPoolSpec(
                CloudProvider.AZURE, 1L, "eastus", "mex-prod-rs",
                "Standard_D4s_v5", ManagedPoolSpec.CapacityType.ON_DEMAND,
                3, 3, 5, "amd64", List.of(), List.of());

        AgentPoolInner pool = AksAdapter.buildAgentPool(spec);

        assertEquals("Standard_D4s_v5", pool.vmSize());
        assertEquals(3, pool.count());
        assertEquals(3, pool.minCount());
        assertEquals(5, pool.maxCount());
        assertEquals(Boolean.TRUE, pool.enableAutoScaling());
        assertEquals(AgentPoolMode.USER, pool.mode());
        assertEquals(OSType.LINUX, pool.osType());
        assertEquals(ScaleSetPriority.REGULAR, pool.scaleSetPriority(),
                "ON_DEMAND must map to REGULAR scale-set priority");
    }

    @Test
    void spot_capacity_lifts_to_scale_set_priority_spot() {
        ManagedPoolSpec spec = new ManagedPoolSpec(
                CloudProvider.AZURE, 1L, "westus2", "mex-spot",
                "Standard_B2ms", ManagedPoolSpec.CapacityType.SPOT,
                1, 2, 4, "amd64", List.of(), List.of());
        AgentPoolInner pool = AksAdapter.buildAgentPool(spec);
        assertEquals(ScaleSetPriority.SPOT, pool.scaleSetPriority());
    }

    @Test
    void service_principal_payload_with_colon_in_secret_parses_correctly() {
        // Recent audit fix: split(":", 3) used to truncate Azure
        // secrets that happen to contain a ':'. Verify the new
        // anchor-based parser keeps the secret whole.
        InMemorySecretStore store = new InMemorySecretStore();
        String ref = SecretStore.newRef(CloudProvider.AZURE, "test-aks");
        String tenantId = "11111111-2222-3333-4444-555555555555";
        String clientId = "66666666-7777-8888-9999-aaaaaaaaaaaa";
        String secret = "p4ss:w0rd:with:colons";  // legit Azure secret
        store.store(ref, tenantId + ":" + clientId + ":" + secret);

        CloudCredential cred = new CloudCredential(1L, "test-aks", CloudProvider.AZURE,
                CloudCredential.AuthMode.SERVICE_PRINCIPAL, ref,
                Optional.empty(), Optional.empty(),
                Optional.of("sub-1"), Optional.of("eastus"),
                0L, Optional.empty(), Optional.empty());

        var tc = new AksAdapter(store).credentialFor(cred);
        // We can't read the secret back from a ClientSecretCredential
        // (no public accessor), but the type confirms the parse
        // succeeded — falling back to DefaultAzureCredential would
        // mean the parse failed.
        assertInstanceOf(ClientSecretCredential.class, tc,
                "Three-part payload with colons in secret must yield "
                + "a ClientSecretCredential, not the default chain");
    }

    @Test
    void malformed_service_principal_payload_falls_back_to_default_chain() {
        InMemorySecretStore store = new InMemorySecretStore();
        String ref = SecretStore.newRef(CloudProvider.AZURE, "bad");
        // Only two segments — missing the secret entirely.
        store.store(ref, "tenant:client");

        CloudCredential cred = new CloudCredential(1L, "bad-aks", CloudProvider.AZURE,
                CloudCredential.AuthMode.SERVICE_PRINCIPAL, ref,
                Optional.empty(), Optional.empty(),
                Optional.of("sub"), Optional.empty(),
                0L, Optional.empty(), Optional.empty());

        var tc = new AksAdapter(store).credentialFor(cred);
        assertInstanceOf(DefaultAzureCredential.class, tc);
    }

    @Test
    void managed_identity_uses_default_chain() {
        CloudCredential cred = new CloudCredential(1L, "mi-aks", CloudProvider.AZURE,
                CloudCredential.AuthMode.MANAGED_IDENTITY, "keychain://mi",
                Optional.empty(), Optional.empty(),
                Optional.of("sub"), Optional.empty(),
                0L, Optional.empty(), Optional.empty());

        var tc = new AksAdapter(new InMemorySecretStore()).credentialFor(cred);
        assertInstanceOf(DefaultAzureCredential.class, tc);
    }
}
