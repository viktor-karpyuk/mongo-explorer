package com.kubrik.mex.k8s.preflight;

import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.preflight.checks.CertManagerCheck;
import com.kubrik.mex.k8s.preflight.checks.ClusterVersionCheck;
import com.kubrik.mex.k8s.preflight.checks.NamespaceCheck;
import com.kubrik.mex.k8s.preflight.checks.NodeFeasibilityCheck;
import com.kubrik.mex.k8s.preflight.checks.OperatorInstalledCheck;
import com.kubrik.mex.k8s.preflight.checks.QuotaCheck;
import com.kubrik.mex.k8s.preflight.checks.RbacCheck;
import com.kubrik.mex.k8s.preflight.checks.StorageClassCheck;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import io.kubernetes.client.openapi.ApiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * v2.8.1 Q2.8.1-G — Runs every pre-flight check against the target
 * cluster + model and folds the results into a {@link
 * PreflightSummary}.
 *
 * <p>Checks run in parallel on a bounded virtual-thread executor so
 * a slow single check can't stall the panel. Global budget is
 * 10 s (spec §6); any check still pending at the budget deadline is
 * recorded as {@link PreflightResult#skipped} with a timeout hint
 * rather than failing the whole Apply path.</p>
 */
public final class PreflightEngine {

    private static final Logger log = LoggerFactory.getLogger(PreflightEngine.class);

    public static final long DEFAULT_BUDGET_MS = 10_000L;

    private final KubeClientFactory clientFactory;
    private final List<PreflightCheck> checks;
    private final long budgetMs;

    public PreflightEngine(KubeClientFactory clientFactory) {
        this(clientFactory, defaultChecks(), DEFAULT_BUDGET_MS);
    }

    /** v2.8.4 constructor — same as the default but with the managed-
     *  pool preflight checks injected. Production callers build this
     *  variant once the {@link CloudCredentialDao} + adapter registry
     *  are wired; older call-sites continue to use the no-arg ctor
     *  and simply skip managed-pool validation. */
    public PreflightEngine(KubeClientFactory clientFactory,
                            com.kubrik.mex.k8s.compute.managedpool.CloudCredentialDao credDao,
                            com.kubrik.mex.k8s.compute.managedpool.ManagedPoolAdapterRegistry registry) {
        this(clientFactory, withManagedPool(defaultChecks(), credDao, registry),
                DEFAULT_BUDGET_MS);
    }

    PreflightEngine(KubeClientFactory clientFactory,
                     List<PreflightCheck> checks, long budgetMs) {
        this.clientFactory = clientFactory;
        this.checks = List.copyOf(checks);
        this.budgetMs = budgetMs;
    }

    private static List<PreflightCheck> withManagedPool(List<PreflightCheck> base,
                                                          com.kubrik.mex.k8s.compute.managedpool.CloudCredentialDao credDao,
                                                          com.kubrik.mex.k8s.compute.managedpool.ManagedPoolAdapterRegistry registry) {
        List<PreflightCheck> out = new ArrayList<>(base);
        out.addAll(new com.kubrik.mex.k8s.compute.managedpool.ManagedPoolPreflightChecks(
                credDao, registry).all());
        return List.copyOf(out);
    }

    public static List<PreflightCheck> defaultChecks() {
        List<PreflightCheck> all = new java.util.ArrayList<>(List.of(
                new OperatorInstalledCheck(),
                new ClusterVersionCheck(),
                new NamespaceCheck(),
                new RbacCheck(),
                new StorageClassCheck(),
                new NodeFeasibilityCheck(),
                new CertManagerCheck(),
                new QuotaCheck()));
        // v2.8.2 Q2.8.2-C — node-pool targeting checks. Each is
        // gated by scope() so a ComputeStrategy.None model skips
        // them quietly.
        all.addAll(com.kubrik.mex.k8s.compute.nodepool.NodePoolPreflightChecks.all());
        // v2.8.3 Q2.8.3-B — Karpenter-strategy checks. Same scope
        // gating: only runs when ComputeStrategy.Karpenter picked.
        all.addAll(com.kubrik.mex.k8s.compute.karpenter.KarpenterPreflightChecks.all());
        return List.copyOf(all);
    }

    public PreflightSummary run(K8sClusterRef clusterRef, ProvisionModel model) {
        ApiClient client;
        try {
            client = clientFactory.get(clusterRef);
        } catch (IOException ioe) {
            return new PreflightSummary(List.of(PreflightResult.fail(
                    "preflight.client",
                    "Could not build ApiClient: " + ioe.getMessage(),
                    "Re-probe the cluster; kubeconfig may be missing.")));
        }

        // try-with-resources so the per-run executor is shut down even
        // if a future's get() throws unexpectedly. Without this every
        // pre-flight run left behind a small amount of ExecutorService
        // bookkeeping state.
        try (ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<PreflightResult>> futures = new ArrayList<>();
            long deadline = System.currentTimeMillis() + budgetMs;
            for (PreflightCheck check : checks) {
                PreflightCheck.PreflightScope scope = check.scope(model);
                if (scope == PreflightCheck.PreflightScope.SKIP) {
                    futures.add(CompletableFuture.completedFuture(
                            PreflightResult.skipped(check.id(), "not applicable")));
                    continue;
                }
                futures.add(CompletableFuture.supplyAsync(() -> {
                    try { return check.run(client, model); }
                    catch (Throwable t) {
                        log.debug("check {} errored: {}", check.id(), t.toString());
                        return PreflightResult.warn(check.id(),
                                "errored: " + t.getMessage(),
                                "Pre-flight degraded to a warning — Apply may still work.");
                    }
                }, exec));
            }

            List<PreflightResult> results = new ArrayList<>(checks.size());
            for (int i = 0; i < futures.size(); i++) {
                long remaining = Math.max(0, deadline - System.currentTimeMillis());
                try {
                    results.add(futures.get(i).get(remaining,
                            java.util.concurrent.TimeUnit.MILLISECONDS));
                } catch (Exception e) {
                    // Cancel the still-running check so the try-with-
                    // resources' close() doesn't block for the full
                    // budget waiting on orphaned virtual threads.
                    futures.get(i).cancel(true);
                    results.add(PreflightResult.skipped(checks.get(i).id(),
                            "timed out within the " + budgetMs + "ms budget"));
                }
            }
            return new PreflightSummary(results);
        }
    }
}
