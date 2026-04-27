package com.kubrik.mex.k8s.compute.karpenter;

import com.kubrik.mex.k8s.compute.ComputeStrategy;
import com.kubrik.mex.k8s.preflight.PreflightCheck;
import com.kubrik.mex.k8s.preflight.PreflightResult;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.ApiextensionsV1Api;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import io.kubernetes.client.openapi.models.V1DeploymentList;

import java.util.List;

/**
 * v2.8.3 Q2.8.3-B — PRE-KP-1..4 checks that gate a Karpenter-backed
 * Apply.
 *
 * <p>All hard-FAIL: a Mongo provision aimed at a cluster where
 * Karpenter isn't installed / running / healthy has no "warn and
 * proceed" value — the pods would stay Pending until someone rips
 * Karpenter out and switches strategies anyway.</p>
 *
 * <p>PRE-KP-3 (NodeClass reachability) isn't wired here because a
 * generic NodeClass probe requires group-specific CRs; it's
 * deferred to the UI's NodeClass picker which can surface the
 * equivalent check during selection.</p>
 */
public final class KarpenterPreflightChecks {

    private KarpenterPreflightChecks() {}

    public static List<PreflightCheck> all() {
        return List.of(
                new CrdsCheck(),
                new ControllerCheck(),
                new NodeClassCheck(),
                new RequirementsCheck());
    }

    /* ============================ PRE-KP-1 ============================ */

    public static final class CrdsCheck implements PreflightCheck {
        public static final String ID = "preflight.karpenter.crds";

        @Override public String id() { return ID; }
        @Override public String label() { return "Karpenter CRDs installed"; }
        @Override public PreflightScope scope(ProvisionModel m) {
            return m.computeStrategy() instanceof ComputeStrategy.Karpenter
                    ? PreflightScope.CONDITIONAL : PreflightScope.SKIP;
        }

        @Override
        public PreflightResult run(ApiClient client, ProvisionModel m) {
            try {
                new ApiextensionsV1Api(client)
                        .readCustomResourceDefinition("nodepools.karpenter.sh")
                        .execute();
                return PreflightResult.pass(ID);
            } catch (ApiException ae) {
                if (ae.getCode() == 404) {
                    return PreflightResult.fail(ID,
                            "NodePool CRD (karpenter.sh/v1) not installed on the cluster.",
                            "Install Karpenter v1 via its Helm chart before retrying — "
                            + "v0 (legacy Provisioner) is not supported.");
                }
                return PreflightResult.fail(ID,
                        "CRD probe failed: HTTP " + ae.getCode(),
                        "Verify the ServiceAccount can read CustomResourceDefinitions.");
            } catch (Exception e) {
                return PreflightResult.fail(ID,
                        "CRD probe errored: " + e.getMessage(),
                        "Cluster unreachable — retry.");
            }
        }
    }

    /* ============================ PRE-KP-2 ============================ */

    public static final class ControllerCheck implements PreflightCheck {
        public static final String ID = "preflight.karpenter.controller";

        @Override public String id() { return ID; }
        @Override public String label() { return "Karpenter controller ready"; }
        @Override public PreflightScope scope(ProvisionModel m) {
            return m.computeStrategy() instanceof ComputeStrategy.Karpenter
                    ? PreflightScope.CONDITIONAL : PreflightScope.SKIP;
        }

        @Override
        public PreflightResult run(ApiClient client, ProvisionModel m) {
            try {
                V1DeploymentList list = new AppsV1Api(client)
                        .listNamespacedDeployment("karpenter")
                        .labelSelector("app.kubernetes.io/name=karpenter")
                        .execute();
                if (list.getItems().isEmpty()) {
                    return PreflightResult.fail(ID,
                            "No Karpenter Deployment found in the 'karpenter' namespace.",
                            "Install the Karpenter controller and re-run pre-flight.");
                }
                boolean anyReady = list.getItems().stream().anyMatch(d -> {
                    var s = d.getStatus();
                    if (s == null || s.getReadyReplicas() == null) return false;
                    return s.getReadyReplicas() > 0;
                });
                if (!anyReady) {
                    return PreflightResult.fail(ID,
                            "Karpenter controller pod(s) are not Ready.",
                            "Check the controller's logs + events; retry once it settles.");
                }
                return PreflightResult.pass(ID);
            } catch (ApiException ae) {
                if (ae.getCode() == 404) {
                    return PreflightResult.fail(ID,
                            "Namespace 'karpenter' not found.",
                            "Re-install Karpenter into the expected namespace (karpenter).");
                }
                return PreflightResult.fail(ID,
                        "Controller probe failed: HTTP " + ae.getCode(),
                        "Verify the ServiceAccount can list Deployments in 'karpenter'.");
            } catch (Exception e) {
                return PreflightResult.fail(ID,
                        "Controller probe errored: " + e.getMessage(),
                        "Retry once the API server is reachable.");
            }
        }
    }

    /* ============================ PRE-KP-3 ============================ */

    public static final class NodeClassCheck implements PreflightCheck {
        public static final String ID = "preflight.karpenter.node-class";

        @Override public String id() { return ID; }
        @Override public String label() { return "NodeClass reachable"; }
        @Override public PreflightScope scope(ProvisionModel m) {
            return m.computeStrategy() instanceof ComputeStrategy.Karpenter k
                    && k.spec().isPresent()
                    ? PreflightScope.CONDITIONAL : PreflightScope.SKIP;
        }

        @Override
        public PreflightResult run(ApiClient client, ProvisionModel m) {
            ComputeStrategy.Karpenter k = (ComputeStrategy.Karpenter) m.computeStrategy();
            KarpenterSpec sp = k.spec().orElseThrow();
            KarpenterSpec.NodeClassRef ref = sp.nodeClassRef();
            String group = groupOf(ref.apiVersion());
            String version = versionOf(ref.apiVersion());
            String plural = pluralOf(ref.kind());
            try {
                new CustomObjectsApi(client)
                        .getClusterCustomObject(group, version, plural, ref.name())
                        .execute();
                return PreflightResult.pass(ID);
            } catch (ApiException ae) {
                if (ae.getCode() == 404) {
                    return PreflightResult.fail(ID,
                            ref.kind() + " '" + ref.name() + "' not found in "
                            + ref.apiVersion() + ".",
                            "Apply the NodeClass manifest before provisioning, "
                            + "or pick an existing one.");
                }
                return PreflightResult.fail(ID,
                        "NodeClass probe failed: HTTP " + ae.getCode(),
                        "Verify the ServiceAccount can read " + ref.kind() + ".");
            } catch (Exception e) {
                return PreflightResult.fail(ID,
                        "NodeClass probe errored: " + e.getMessage(),
                        "Retry once the API server is reachable.");
            }
        }

        private static String groupOf(String apiVersion) {
            int slash = apiVersion.indexOf('/');
            return slash < 0 ? "" : apiVersion.substring(0, slash);
        }

        private static String versionOf(String apiVersion) {
            int slash = apiVersion.indexOf('/');
            return slash < 0 ? apiVersion : apiVersion.substring(slash + 1);
        }

        /** Best-effort pluralisation for the well-known NodeClass kinds.
         *  Falls back to lowercase + "s" — adequate for v2.8.3 which
         *  only blesses EC2NodeClass (AWS), AKSNodeClass (Azure
         *  preview), and a generic fallback. */
        private static String pluralOf(String kind) {
            return switch (kind) {
                case "EC2NodeClass" -> "ec2nodeclasses";
                case "AKSNodeClass" -> "aksnodeclasses";
                case "GCENodeClass" -> "gcenodeclasses";
                default -> kind.toLowerCase() + "s";
            };
        }
    }

    /* ============================ PRE-KP-4 ============================ */

    public static final class RequirementsCheck implements PreflightCheck {
        public static final String ID = "preflight.karpenter.requirements";

        @Override public String id() { return ID; }
        @Override public String label() { return "Karpenter NodePool requirements"; }
        @Override public PreflightScope scope(ProvisionModel m) {
            return m.computeStrategy() instanceof ComputeStrategy.Karpenter
                    ? PreflightScope.CONDITIONAL : PreflightScope.SKIP;
        }

        @Override
        public PreflightResult run(ApiClient client, ProvisionModel m) {
            ComputeStrategy.Karpenter k = (ComputeStrategy.Karpenter) m.computeStrategy();
            if (k.spec().isEmpty()) {
                return PreflightResult.fail(ID,
                        "Karpenter strategy selected but no spec built.",
                        "Re-open the wizard's Dedicated-compute step and fill the form.");
            }
            KarpenterSpec sp = k.spec().get();
            if (!sp.hasMinimumRequirements()) {
                return PreflightResult.fail(ID,
                        "NodePool requirements must include at least one capacity-type "
                        + "and one architecture.",
                        "Add values in the Dedicated-compute step and retry.");
            }
            return PreflightResult.pass(ID);
        }
    }
}
