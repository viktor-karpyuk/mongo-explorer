package com.kubrik.mex.k8s.compute.managedpool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eks.EksClient;
import software.amazon.awssdk.services.eks.model.AMITypes;
import software.amazon.awssdk.services.eks.model.CapacityTypes;
import software.amazon.awssdk.services.eks.model.CreateNodegroupRequest;
import software.amazon.awssdk.services.eks.model.CreateNodegroupResponse;
import software.amazon.awssdk.services.eks.model.DeleteNodegroupRequest;
import software.amazon.awssdk.services.eks.model.DeleteNodegroupResponse;
import software.amazon.awssdk.services.eks.model.DescribeNodegroupRequest;
import software.amazon.awssdk.services.eks.model.DescribeNodegroupResponse;
import software.amazon.awssdk.services.eks.model.NodegroupScalingConfig;
import software.amazon.awssdk.services.eks.model.NodegroupStatus;
import software.amazon.awssdk.services.eks.model.ResourceNotFoundException;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.4.0 — Real AWS SDK-backed EKS managed-pool adapter.
 *
 * <p>Replaces {@link EksAdapterStub} once the cloud-credentials
 * subsystem ({@link CloudCredentialDao} + {@link SecretStore}) is
 * security-reviewed. Wiring is opt-in via
 * {@link #wireInto(ManagedPoolAdapterRegistry, SecretStore, String)}
 * so a build that hasn't completed the review keeps using the stub.</p>
 *
 * <p>Auth modes (matching {@link CloudCredential.AuthMode}):</p>
 * <ul>
 *   <li>{@code IRSA} / {@code SSO_PROFILE} — fall through to
 *       {@link DefaultCredentialsProvider} (the SDK's standard chain
 *       — env vars, ~/.aws/credentials, IRSA, instance metadata).</li>
 *   <li>{@code EXTERNAL_ID} — read AWS profile name from the credential's
 *       {@code keychainRef} payload via {@link SecretStore}; resolves
 *       through {@link ProfileCredentialsProvider}.</li>
 *   <li>{@code STATIC} — keychain payload is {@code accessKey:secretKey},
 *       wired through {@link StaticCredentialsProvider}.</li>
 * </ul>
 *
 * <p>Cluster name is derived from {@link ManagedPoolSpec#poolName} —
 * the EKS API requires a separate cluster + nodegroup name pair. We
 * adopt the convention {@code <poolName>-cluster} when the user
 * doesn't override; production wires usually have a single cluster
 * per credential and the wizard fills it from the connected cluster
 * row. Until the wizard surface for that lands, the adapter accepts
 * a system property {@code mex.eks.cluster} as an override.</p>
 */
public final class EksAdapter implements ManagedPoolAdapter {

    private static final Logger log = LoggerFactory.getLogger(EksAdapter.class);

    private final SecretStore secrets;
    /** Required by EKS — the IAM role the nodegroup nodes assume.
     *  Operator supplies via {@code mex.eks.node_role_arn}; the wizard
     *  surface for this lands with the full credential pane. */
    private final String defaultNodeRoleArn;

    public EksAdapter(SecretStore secrets, String defaultNodeRoleArn) {
        this.secrets = Objects.requireNonNull(secrets, "secrets");
        this.defaultNodeRoleArn = defaultNodeRoleArn;
    }

    @Override public CloudProvider provider() { return CloudProvider.AWS; }

    @Override
    public PoolOperationResult createPool(CloudCredential credential, ManagedPoolSpec spec) {
        try (EksClient eks = clientFor(credential, spec)) {
            CreateNodegroupRequest req = CreateNodegroupRequest.builder()
                    .clusterName(clusterName(spec))
                    .nodegroupName(spec.poolName())
                    .scalingConfig(NodegroupScalingConfig.builder()
                            .minSize(spec.minNodes())
                            .desiredSize(spec.desiredNodes())
                            .maxSize(spec.maxNodes())
                            .build())
                    .instanceTypes(spec.instanceType())
                    .capacityType(spec.capacityType()
                            == ManagedPoolSpec.CapacityType.SPOT
                            ? CapacityTypes.SPOT : CapacityTypes.ON_DEMAND)
                    .amiType("arm64".equals(spec.arch())
                            ? AMITypes.AL2_ARM_64 : AMITypes.AL2_X86_64)
                    .nodeRole(resolveNodeRoleArn())
                    .subnets(spec.subnetIds())
                    .build();
            CreateNodegroupResponse resp = eks.createNodegroup(req);
            log.info("EKS createNodegroup {} -> arn={}", spec.poolName(),
                    resp.nodegroup() == null ? "?" : resp.nodegroup().nodegroupArn());
            return PoolOperationResult.accepted(resp.nodegroup() == null
                    ? null : resp.nodegroup().nodegroupArn());
        } catch (Exception e) {
            log.warn("EKS createNodegroup failed: {}", e.toString());
            return PoolOperationResult.rejected(e.getMessage());
        }
    }

    @Override
    public Optional<PoolDescription> describe(CloudCredential credential,
                                                String region, String poolName) {
        try (EksClient eks = clientFor(credential, region)) {
            String cluster = System.getProperty("mex.eks.cluster", poolName + "-cluster");
            DescribeNodegroupResponse resp = eks.describeNodegroup(
                    DescribeNodegroupRequest.builder()
                            .clusterName(cluster)
                            .nodegroupName(poolName)
                            .build());
            if (resp.nodegroup() == null) return Optional.empty();
            NodegroupStatus status = resp.nodegroup().status();
            int ready = resp.nodegroup().scalingConfig() == null ? 0
                    : resp.nodegroup().scalingConfig().desiredSize();
            return Optional.of(new PoolDescription(poolName, mapPhase(status), ready));
        } catch (ResourceNotFoundException nf) {
            return Optional.empty();
        } catch (Exception e) {
            log.debug("EKS describeNodegroup {}: {}", poolName, e.toString());
            return Optional.empty();
        }
    }

    @Override
    public PoolOperationResult deletePool(CloudCredential credential,
                                            String region, String poolName) {
        try (EksClient eks = clientFor(credential, region)) {
            String cluster = System.getProperty("mex.eks.cluster", poolName + "-cluster");
            DeleteNodegroupResponse resp = eks.deleteNodegroup(
                    DeleteNodegroupRequest.builder()
                            .clusterName(cluster)
                            .nodegroupName(poolName)
                            .build());
            return PoolOperationResult.accepted(resp.nodegroup() == null
                    ? null : resp.nodegroup().nodegroupArn());
        } catch (Exception e) {
            log.warn("EKS deleteNodegroup failed: {}", e.toString());
            return PoolOperationResult.rejected(e.getMessage());
        }
    }

    /* ============================ wiring helpers ============================ */

    /** Drop-in replace the EKS stub on the production registry. Call
     *  from MainView once the cloud-credentials pane is wired and
     *  security-reviewed. */
    public static void wireInto(ManagedPoolAdapterRegistry registry,
                                  SecretStore secrets, String defaultNodeRoleArn) {
        registry.register(new EksAdapter(secrets, defaultNodeRoleArn));
    }

    private EksClient clientFor(CloudCredential credential, ManagedPoolSpec spec) {
        return clientFor(credential, spec.region());
    }

    private EksClient clientFor(CloudCredential credential, String region) {
        return EksClient.builder()
                .region(Region.of(region))
                .credentialsProvider(providerFor(credential))
                .httpClient(UrlConnectionHttpClient.builder().build())
                .build();
    }

    private AwsCredentialsProvider providerFor(CloudCredential cred) {
        return switch (cred.authMode()) {
            case STATIC -> {
                String body = secrets.read(cred.keychainRef()).orElse("");
                int colon = body.indexOf(':');
                if (colon <= 0) {
                    log.warn("STATIC credential {} keychain payload has no ':' separator",
                            cred.displayName());
                    yield DefaultCredentialsProvider.create();
                }
                yield StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        body.substring(0, colon), body.substring(colon + 1)));
            }
            case EXTERNAL_ID -> {
                String profile = secrets.read(cred.keychainRef()).orElse("default");
                yield ProfileCredentialsProvider.create(profile);
            }
            default -> DefaultCredentialsProvider.create();
        };
    }

    private String clusterName(ManagedPoolSpec spec) {
        return System.getProperty("mex.eks.cluster", spec.poolName() + "-cluster");
    }

    private String resolveNodeRoleArn() {
        String fromProp = System.getProperty("mex.eks.node_role_arn");
        if (fromProp != null && !fromProp.isBlank()) return fromProp;
        if (defaultNodeRoleArn != null && !defaultNodeRoleArn.isBlank()) {
            return defaultNodeRoleArn;
        }
        throw new IllegalStateException(
                "mex.eks.node_role_arn system property required (or pass via "
                + "EksAdapter constructor) — EKS createNodegroup needs the "
                + "node IAM role ARN.");
    }

    private static PoolPhase mapPhase(NodegroupStatus status) {
        if (status == null) return PoolPhase.ABSENT;
        return switch (status) {
            case CREATING -> PoolPhase.CREATING;
            case ACTIVE -> PoolPhase.READY;
            case UPDATING -> PoolPhase.UPDATING;
            case DELETING -> PoolPhase.DELETING;
            case CREATE_FAILED, DELETE_FAILED, DEGRADED -> PoolPhase.FAILED;
            default -> PoolPhase.UPDATING;
        };
    }
}
