package com.kubrik.mex.k8s.compute.managedpool;

import com.kubrik.mex.k8s.compute.ComputeStrategy;
import com.kubrik.mex.k8s.preflight.PreflightCheck;
import com.kubrik.mex.k8s.preflight.PreflightResult;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import io.kubernetes.client.openapi.ApiClient;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.4 Q2.8.4-C — PRE-MP-1..5 checks that gate a managed-pool
 * Apply.
 *
 * <p>These run against Mongo Explorer's local DAOs + the
 * {@link ManagedPoolAdapterRegistry}; no cloud API calls are issued
 * during pre-flight because cross-region latency + rate limits make
 * that surface hostile in a preview path. The adapter's probe call
 * fires during Apply, recorded to {@code managed_pool_operations}.</p>
 */
public final class ManagedPoolPreflightChecks {

    private final CloudCredentialDao credDao;
    private final ManagedPoolAdapterRegistry registry;

    public ManagedPoolPreflightChecks(CloudCredentialDao credDao,
                                        ManagedPoolAdapterRegistry registry) {
        this.credDao = Objects.requireNonNull(credDao, "credDao");
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    public List<PreflightCheck> all() {
        return List.of(
                new CredentialPresentCheck(credDao),
                new AdapterRegisteredCheck(registry),
                new SpecShapeCheck());
    }

    /* ============================ PRE-MP-1 ============================ */

    public static final class CredentialPresentCheck implements PreflightCheck {
        public static final String ID = "preflight.managed-pool.credential";
        private final CloudCredentialDao credDao;

        public CredentialPresentCheck(CloudCredentialDao credDao) {
            this.credDao = credDao;
        }

        @Override public String id() { return ID; }
        @Override public String label() { return "Cloud credential exists"; }
        @Override public PreflightScope scope(ProvisionModel m) {
            return m.computeStrategy() instanceof ComputeStrategy.ManagedPool
                    ? PreflightScope.CONDITIONAL : PreflightScope.SKIP;
        }

        @Override
        public PreflightResult run(ApiClient client, ProvisionModel m) {
            ComputeStrategy.ManagedPool mp = (ComputeStrategy.ManagedPool) m.computeStrategy();
            if (mp.spec().isEmpty()) {
                return PreflightResult.fail(ID,
                        "Managed-pool strategy selected but no spec built.",
                        "Re-open the wizard's Dedicated-compute step and pick a credential.");
            }
            ManagedPoolSpec sp = mp.spec().get();
            try {
                Optional<CloudCredential> cred = credDao.findById(sp.credentialId());
                if (cred.isEmpty()) {
                    return PreflightResult.fail(ID,
                            "Cloud credential id " + sp.credentialId() + " not found.",
                            "Add it under Cloud credentials and re-select it in the wizard.");
                }
                if (cred.get().provider() != sp.provider()) {
                    return PreflightResult.fail(ID,
                            "Credential provider " + cred.get().provider()
                            + " does not match the picked pool provider " + sp.provider() + ".",
                            "Pick a credential from the same cloud as the pool.");
                }
                return PreflightResult.pass(ID);
            } catch (SQLException sqle) {
                return PreflightResult.fail(ID,
                        "Credential lookup failed: " + sqle.getMessage(),
                        "Retry once the local database is reachable.");
            }
        }
    }

    /* ============================ PRE-MP-2 ============================ */

    public static final class AdapterRegisteredCheck implements PreflightCheck {
        public static final String ID = "preflight.managed-pool.adapter";
        private final ManagedPoolAdapterRegistry registry;

        public AdapterRegisteredCheck(ManagedPoolAdapterRegistry registry) {
            this.registry = registry;
        }

        @Override public String id() { return ID; }
        @Override public String label() { return "Cloud adapter shipped"; }
        @Override public PreflightScope scope(ProvisionModel m) {
            return m.computeStrategy() instanceof ComputeStrategy.ManagedPool
                    ? PreflightScope.CONDITIONAL : PreflightScope.SKIP;
        }

        @Override
        public PreflightResult run(ApiClient client, ProvisionModel m) {
            ComputeStrategy.ManagedPool mp = (ComputeStrategy.ManagedPool) m.computeStrategy();
            if (mp.spec().isEmpty()) return PreflightResult.pass(ID); // deferred to PRE-MP-1
            CloudProvider p = mp.spec().get().provider();
            if (registry.lookup(p).isEmpty()) {
                return PreflightResult.fail(ID,
                        "No adapter for cloud provider " + p + " is linked into this build.",
                        "Re-run once the " + p + " adapter ships (GKE = v2.8.4.1, AKS = v2.8.4.2).");
            }
            return PreflightResult.pass(ID);
        }
    }

    /* ============================ PRE-MP-4 ============================ */

    public static final class SpecShapeCheck implements PreflightCheck {
        public static final String ID = "preflight.managed-pool.spec";

        @Override public String id() { return ID; }
        @Override public String label() { return "Managed-pool spec shape"; }
        @Override public PreflightScope scope(ProvisionModel m) {
            return m.computeStrategy() instanceof ComputeStrategy.ManagedPool
                    ? PreflightScope.CONDITIONAL : PreflightScope.SKIP;
        }

        @Override
        public PreflightResult run(ApiClient client, ProvisionModel m) {
            ComputeStrategy.ManagedPool mp = (ComputeStrategy.ManagedPool) m.computeStrategy();
            if (mp.spec().isEmpty()) return PreflightResult.pass(ID);
            ManagedPoolSpec sp = mp.spec().get();
            int needed = m.topology().replicasPerReplset();
            if (sp.maxNodes() < needed) {
                return PreflightResult.fail(ID,
                        "Pool max (" + sp.maxNodes() + " nodes) is below the topology's "
                        + needed + " replicas.",
                        "Raise max-nodes in the Dedicated-compute step.");
            }
            if (sp.region().isBlank()) {
                return PreflightResult.fail(ID,
                        "Pool region is blank.",
                        "Pick the cluster's region in the Dedicated-compute step.");
            }
            return PreflightResult.pass(ID);
        }
    }
}
