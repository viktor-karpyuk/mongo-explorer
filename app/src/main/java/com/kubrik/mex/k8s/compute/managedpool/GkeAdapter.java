package com.kubrik.mex.k8s.compute.managedpool;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.container.v1.ClusterManagerClient;
import com.google.cloud.container.v1.ClusterManagerSettings;
import com.google.container.v1.CreateNodePoolRequest;
import com.google.container.v1.DeleteNodePoolRequest;
import com.google.container.v1.GetNodePoolRequest;
import com.google.container.v1.NodeConfig;
import com.google.container.v1.NodePool;
import com.google.container.v1.NodePoolAutoscaling;
import com.google.container.v1.Operation;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.4.1 — Real GCP SDK-backed GKE managed-pool adapter.
 *
 * <p>Uses Google's Cluster Manager Java client. Replaces
 * {@link GkeAdapterStub} when {@code mex.cloud.real_adapters=true}.</p>
 *
 * <p>Auth modes:</p>
 * <ul>
 *   <li>{@code WORKLOAD_IDENTITY} / {@code APPLICATION_DEFAULT} —
 *       fall through to {@link GoogleCredentials#getApplicationDefault()}.</li>
 *   <li>{@code SERVICE_ACCOUNT_KEY} — keychain payload is the JSON
 *       service-account key body, parsed via
 *       {@link ServiceAccountCredentials#fromStream}.</li>
 * </ul>
 *
 * <p>The GKE cluster name + zone/region come from
 * {@code mex.gke.cluster} and {@code mex.gke.location} until the
 * full GKE sub-form lands (the credential's {@code defaultRegion}
 * fallback is honoured for {@code location}).</p>
 */
public final class GkeAdapter implements ManagedPoolAdapter {

    private static final Logger log = LoggerFactory.getLogger(GkeAdapter.class);
    private static final List<String> SCOPES =
            List.of("https://www.googleapis.com/auth/cloud-platform");

    private final SecretStore secrets;

    public GkeAdapter(SecretStore secrets) {
        this.secrets = Objects.requireNonNull(secrets, "secrets");
    }

    @Override public CloudProvider provider() { return CloudProvider.GCP; }

    @Override
    public PoolOperationResult createPool(CloudCredential credential, ManagedPoolSpec spec) {
        try (ClusterManagerClient client = newClient(credential)) {
            String parent = parentFor(credential, spec.region());
            CreateNodePoolRequest req = buildCreateRequest(spec, parent);
            Operation op = client.createNodePool(req);
            log.info("GKE createNodePool {} -> {}", spec.poolName(), op.getName());
            return PoolOperationResult.accepted(op.getName());
        } catch (Exception e) {
            log.warn("GKE createNodePool failed: {}", e.toString());
            return PoolOperationResult.rejected(e.getMessage());
        }
    }

    /** Visible for tests — assembles the {@code CreateNodePoolRequest}
     *  proto from a spec + GKE parent name without opening a live SDK
     *  client. Lets unit tests assert the wire shape. */
    static CreateNodePoolRequest buildCreateRequest(ManagedPoolSpec spec, String parent) {
        NodePool pool = NodePool.newBuilder()
                .setName(spec.poolName())
                .setInitialNodeCount(spec.desiredNodes())
                .setAutoscaling(NodePoolAutoscaling.newBuilder()
                        .setEnabled(true)
                        .setMinNodeCount(spec.minNodes())
                        .setMaxNodeCount(spec.maxNodes())
                        .build())
                .setConfig(NodeConfig.newBuilder()
                        .setMachineType(spec.instanceType())
                        .setSpot(spec.capacityType()
                                == ManagedPoolSpec.CapacityType.SPOT)
                        .build())
                .build();
        return CreateNodePoolRequest.newBuilder()
                .setParent(parent)
                .setNodePool(pool)
                .build();
    }

    @Override
    public Optional<PoolDescription> describe(CloudCredential credential,
                                                String region, String poolName) {
        try (ClusterManagerClient client = newClient(credential)) {
            String name = parentFor(credential, region) + "/nodePools/" + poolName;
            NodePool pool = client.getNodePool(
                    GetNodePoolRequest.newBuilder().setName(name).build());
            if (pool == null) return Optional.empty();
            return Optional.of(new PoolDescription(poolName,
                    mapPhase(pool.getStatus()),
                    pool.getInitialNodeCount()));
        } catch (StatusRuntimeException sre) {
            // NOT_FOUND surfaces here on a deleted / never-created pool.
            return Optional.empty();
        } catch (Exception e) {
            log.debug("GKE getNodePool {}: {}", poolName, e.toString());
            return Optional.empty();
        }
    }

    @Override
    public PoolOperationResult deletePool(CloudCredential credential,
                                            String region, String poolName) {
        try (ClusterManagerClient client = newClient(credential)) {
            String name = parentFor(credential, region) + "/nodePools/" + poolName;
            Operation op = client.deleteNodePool(
                    DeleteNodePoolRequest.newBuilder().setName(name).build());
            return PoolOperationResult.accepted(op.getName());
        } catch (Exception e) {
            log.warn("GKE deleteNodePool failed: {}", e.toString());
            return PoolOperationResult.rejected(e.getMessage());
        }
    }

    /* ============================ wiring helpers ============================ */

    public static void wireInto(ManagedPoolAdapterRegistry registry, SecretStore secrets) {
        registry.register(new GkeAdapter(secrets));
    }

    private ClusterManagerClient newClient(CloudCredential cred) throws Exception {
        ClusterManagerSettings settings = ClusterManagerSettings.newBuilder()
                .setCredentialsProvider(providerFor(cred))
                .build();
        return ClusterManagerClient.create(settings);
    }

    /** Visible for tests — auth-mode dispatch + payload parsing. */
    CredentialsProvider providerFor(CloudCredential cred) throws Exception {
        if (cred.authMode() == CloudCredential.AuthMode.SERVICE_ACCOUNT_KEY) {
            String body = secrets.read(cred.keychainRef()).orElse("");
            if (body.isBlank()) {
                log.warn("SERVICE_ACCOUNT_KEY credential {} keychain payload empty",
                        cred.displayName());
                return GoogleCredentialsProvider.newBuilder()
                        .setScopesToApply(SCOPES).build();
            }
            ServiceAccountCredentials sa = ServiceAccountCredentials.fromStream(
                    new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));
            return FixedCredentialsProvider.create(sa.createScoped(SCOPES));
        }
        return GoogleCredentialsProvider.newBuilder()
                .setScopesToApply(SCOPES).build();
    }

    private String parentFor(CloudCredential cred, String location) {
        String project = cred.gcpProject().orElse(
                System.getProperty("mex.gke.project", ""));
        String cluster = System.getProperty("mex.gke.cluster", "default");
        if (project.isBlank()) {
            throw new IllegalStateException(
                    "GKE adapter needs the project id — set CloudCredential.gcpProject "
                    + "or pass mex.gke.project system property.");
        }
        return "projects/" + project + "/locations/" + location
                + "/clusters/" + cluster;
    }

    private static PoolPhase mapPhase(NodePool.Status status) {
        if (status == null) return PoolPhase.ABSENT;
        return switch (status) {
            case PROVISIONING -> PoolPhase.CREATING;
            case RUNNING -> PoolPhase.READY;
            case RECONCILING -> PoolPhase.UPDATING;
            case STOPPING -> PoolPhase.DELETING;
            case ERROR, RUNNING_WITH_ERROR -> PoolPhase.FAILED;
            default -> PoolPhase.UPDATING;
        };
    }
}
