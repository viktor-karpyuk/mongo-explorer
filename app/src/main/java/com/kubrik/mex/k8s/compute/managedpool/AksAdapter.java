package com.kubrik.mex.k8s.compute.managedpool;

import com.azure.core.credential.TokenCredential;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.resourcemanager.containerservice.ContainerServiceManager;
import com.azure.resourcemanager.containerservice.fluent.models.AgentPoolInner;
import com.azure.resourcemanager.containerservice.models.AgentPoolMode;
import com.azure.resourcemanager.containerservice.models.OSType;
import com.azure.resourcemanager.containerservice.models.ScaleSetPriority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.4.2 — Real Azure SDK-backed AKS managed-pool adapter.
 *
 * <p>Uses {@code azure-resourcemanager-containerservice} for the AKS
 * Agent Pools API and {@code azure-identity} for the credential
 * chain. Replaces {@link AksAdapterStub} when
 * {@code mex.cloud.real_adapters=true}.</p>
 *
 * <p>Auth modes:</p>
 * <ul>
 *   <li>{@code MANAGED_IDENTITY} — falls through to
 *       {@link DefaultAzureCredentialBuilder} (covers MI on the
 *       VM, env vars, Visual Studio Code login, etc.).</li>
 *   <li>{@code SERVICE_PRINCIPAL} — keychain payload is
 *       {@code tenantId:clientId:secret}, wired through
 *       {@link ClientSecretCredentialBuilder}.</li>
 * </ul>
 *
 * <p>Resource group + cluster name come from
 * {@code mex.aks.resource_group} and {@code mex.aks.cluster} until
 * the full AKS sub-form lands.</p>
 */
public final class AksAdapter implements ManagedPoolAdapter {

    private static final Logger log = LoggerFactory.getLogger(AksAdapter.class);

    private final SecretStore secrets;

    public AksAdapter(SecretStore secrets) {
        this.secrets = Objects.requireNonNull(secrets, "secrets");
    }

    @Override public CloudProvider provider() { return CloudProvider.AZURE; }

    @Override
    public PoolOperationResult createPool(CloudCredential credential, ManagedPoolSpec spec) {
        try {
            ContainerServiceManager mgr = managerFor(credential);
            String rg = resourceGroup();
            String cluster = clusterName();
            AgentPoolInner pool = buildAgentPool(spec);
            AgentPoolInner created = mgr.serviceClient().getAgentPools()
                    .createOrUpdate(rg, cluster, spec.poolName(), pool);
            log.info("AKS createOrUpdate {} -> id={}", spec.poolName(), created.id());
            return PoolOperationResult.accepted(created.id());
        } catch (Exception e) {
            log.warn("AKS createOrUpdate failed: {}", e.toString());
            return PoolOperationResult.rejected(e.getMessage());
        }
    }

    /** Visible for tests — assembles the {@code AgentPoolInner} body
     *  from a spec without opening a live ARM client. Lets unit tests
     *  assert the wire shape (vm size, scale-set priority, OS type,
     *  autoscale bounds). */
    static AgentPoolInner buildAgentPool(ManagedPoolSpec spec) {
        return new AgentPoolInner()
                .withCount(spec.desiredNodes())
                .withMinCount(spec.minNodes())
                .withMaxCount(spec.maxNodes())
                .withEnableAutoScaling(true)
                .withVmSize(spec.instanceType())
                .withMode(AgentPoolMode.USER)
                .withOsType(OSType.LINUX)
                .withScaleSetPriority(spec.capacityType()
                        == ManagedPoolSpec.CapacityType.SPOT
                        ? ScaleSetPriority.SPOT : ScaleSetPriority.REGULAR);
    }

    @Override
    public Optional<PoolDescription> describe(CloudCredential credential,
                                                String region, String poolName) {
        try {
            ContainerServiceManager mgr = managerFor(credential);
            AgentPoolInner pool = mgr.serviceClient().getAgentPools()
                    .get(resourceGroup(), clusterName(), poolName);
            if (pool == null) return Optional.empty();
            String state = pool.provisioningState();
            return Optional.of(new PoolDescription(poolName,
                    mapPhase(state),
                    pool.count() == null ? 0 : pool.count()));
        } catch (Exception e) {
            log.debug("AKS get {}: {}", poolName, e.toString());
            return Optional.empty();
        }
    }

    @Override
    public PoolOperationResult deletePool(CloudCredential credential,
                                            String region, String poolName) {
        try {
            ContainerServiceManager mgr = managerFor(credential);
            mgr.serviceClient().getAgentPools()
                    .delete(resourceGroup(), clusterName(), poolName);
            return PoolOperationResult.accepted("aks-delete-" + poolName);
        } catch (Exception e) {
            log.warn("AKS delete failed: {}", e.toString());
            return PoolOperationResult.rejected(e.getMessage());
        }
    }

    /* ============================ wiring helpers ============================ */

    public static void wireInto(ManagedPoolAdapterRegistry registry, SecretStore secrets) {
        registry.register(new AksAdapter(secrets));
    }

    private ContainerServiceManager managerFor(CloudCredential cred) {
        String subscription = cred.azureSubscription().orElse(
                System.getProperty("mex.aks.subscription", ""));
        if (subscription.isBlank()) {
            throw new IllegalStateException(
                    "AKS adapter needs the subscription id — set "
                    + "CloudCredential.azureSubscription or pass "
                    + "mex.aks.subscription system property.");
        }
        AzureProfile profile = new AzureProfile(
                /* tenantId */ null,
                subscription,
                AzureEnvironment.AZURE);
        return ContainerServiceManager.authenticate(credentialFor(cred), profile);
    }

    /** Visible for tests — auth-mode dispatch + payload parsing. */
    TokenCredential credentialFor(CloudCredential cred) {
        if (cred.authMode() == CloudCredential.AuthMode.SERVICE_PRINCIPAL) {
            String body = secrets.read(cred.keychainRef()).orElse("");
            // Azure client secrets can legitimately contain ':' (rare but
            // legal in newer secret formats). Split at the first two
            // colons by hand so the third segment keeps any trailing
            // colons intact.
            int firstColon = body.indexOf(':');
            int secondColon = firstColon < 0 ? -1 : body.indexOf(':', firstColon + 1);
            if (firstColon <= 0 || secondColon <= firstColon + 1
                    || secondColon >= body.length() - 1) {
                log.warn("SERVICE_PRINCIPAL credential {} payload not "
                        + "tenantId:clientId:secret — falling back to default chain",
                        cred.displayName());
                return new DefaultAzureCredentialBuilder().build();
            }
            String tenantId = body.substring(0, firstColon);
            String clientId = body.substring(firstColon + 1, secondColon);
            String secret = body.substring(secondColon + 1);
            return new ClientSecretCredentialBuilder()
                    .tenantId(tenantId)
                    .clientId(clientId)
                    .clientSecret(secret)
                    .build();
        }
        return new DefaultAzureCredentialBuilder().build();
    }

    private static String resourceGroup() {
        String rg = System.getProperty("mex.aks.resource_group");
        if (rg == null || rg.isBlank()) {
            throw new IllegalStateException(
                    "mex.aks.resource_group system property required");
        }
        return rg;
    }

    private static String clusterName() {
        String c = System.getProperty("mex.aks.cluster");
        if (c == null || c.isBlank()) {
            throw new IllegalStateException(
                    "mex.aks.cluster system property required");
        }
        return c;
    }

    private static PoolPhase mapPhase(String provisioningState) {
        if (provisioningState == null) return PoolPhase.ABSENT;
        return switch (provisioningState.toLowerCase()) {
            case "creating" -> PoolPhase.CREATING;
            case "succeeded" -> PoolPhase.READY;
            case "updating", "scaling", "upgrading" -> PoolPhase.UPDATING;
            case "deleting" -> PoolPhase.DELETING;
            case "failed", "canceled" -> PoolPhase.FAILED;
            default -> PoolPhase.UPDATING;
        };
    }
}
