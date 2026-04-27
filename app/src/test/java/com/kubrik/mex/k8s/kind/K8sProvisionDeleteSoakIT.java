package com.kubrik.mex.k8s.kind;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.k8s.apply.ApplyOrchestrator;
import com.kubrik.mex.k8s.apply.ProvisioningRecordDao;
import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.client.KubeConfigLoader;
import com.kubrik.mex.k8s.cluster.KubeClusterDao;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.operator.mco.McoAdapter;
import com.kubrik.mex.k8s.provision.Profile;
import com.kubrik.mex.k8s.provision.ProfileEnforcer;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.k8s.rollout.RolloutEventDao;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * v2.8.1 Q2.8-L3 — 72-hour soak harness.
 *
 * <p>Loops {@code provision → delete} against a kind cluster with
 * the operators pre-installed (testing/kind/install-operators.sh),
 * stopping when either the wall-clock budget elapses or the
 * iteration cap is hit.</p>
 *
 * <p>Skipped by default — runs only when both:</p>
 * <ul>
 *   <li>{@code MEX_K8S_IT=kind}</li>
 *   <li>{@code MEX_SOAK=72h} (or the iteration override
 *       {@code MEX_SOAK_ITERATIONS=<int>})</li>
 * </ul>
 *
 * <p>Tagged {@code k8sSoak} so it runs only via the dedicated gradle
 * task ({@code :app:k8sSoakTest}) and never on the normal test run.</p>
 *
 * <p>Each loop creates a unique deployment name (suffixed with the
 * iteration index + epoch ms) to avoid name collisions on a kind
 * cluster that's also being soaked elsewhere; the cleanup leg is
 * called even on apply failure so a half-applied iteration doesn't
 * leak a CR into the next loop.</p>
 */
@Tag("k8sSoak")
@EnabledIfEnvironmentVariable(named = "MEX_K8S_IT", matches = "kind")
class K8sProvisionDeleteSoakIT {

    private static final Duration BUDGET = parseBudget(
            System.getenv("MEX_SOAK"));
    private static final int ITERATION_CAP = parseInt(
            System.getenv("MEX_SOAK_ITERATIONS"), 0);

    @TempDir Path dataDir;
    private Database db;
    private ProvisioningRecordDao recordDao;
    private RolloutEventDao eventDao;
    private long clusterId;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        recordDao = new ProvisioningRecordDao(db);
        eventDao = new RolloutEventDao(db);
        clusterId = new KubeClusterDao(db).insert(
                "kind-soak", "/kube", "ctx",
                Optional.empty(), Optional.empty());
    }

    @AfterEach
    void tearDown() throws Exception { if (db != null) db.close(); }

    @Test
    void provision_delete_loop() throws Exception {
        if (BUDGET == null && ITERATION_CAP <= 0) {
            // Both gates open by default — when the user opted in to
            // a soak run via MEX_K8S_IT they need a budget too.
            return;
        }

        K8sClusterRef ref = resolveKindRef();
        ApplyOrchestrator orch = new ApplyOrchestrator(
                new KubeClientFactory(), recordDao, eventDao, new EventBus());

        long deadline = BUDGET == null ? Long.MAX_VALUE
                : System.currentTimeMillis() + BUDGET.toMillis();
        int iterations = 0;
        int failures = 0;
        Instant start = Instant.now();

        while (System.currentTimeMillis() < deadline
                && (ITERATION_CAP == 0 || iterations < ITERATION_CAP)) {
            String deployment = "soak-" + iterations + "-"
                    + System.currentTimeMillis();
            ProvisionModel m = new ProfileEnforcer().switchProfile(
                    ProvisionModel.defaults(clusterId, "mongo-soak", deployment),
                    Profile.DEV_TEST).model();

            ApplyOrchestrator.ApplyResult r;
            try {
                r = orch.apply(ref, new McoAdapter(), m);
            } catch (Exception apply) {
                failures++;
                continue;
            }
            // Always tear down what landed — even a partial failure
            // leaves a catalogue we should drain.
            try { orch.cleanup(ref, r.catalogue()); }
            catch (Exception ignored) { /* surface via record DAO */ }
            if (!r.ok()) failures++;

            iterations++;
            if (iterations % 100 == 0) {
                System.out.println("[soak] iter=" + iterations
                        + " failures=" + failures
                        + " elapsed=" + Duration.between(start, Instant.now()));
            }
        }
        System.out.println("[soak] DONE iter=" + iterations
                + " failures=" + failures
                + " elapsed=" + Duration.between(start, Instant.now()));
        if (failures > 0) {
            throw new AssertionError("soak saw " + failures
                    + " failure(s) over " + iterations + " iteration(s)");
        }
    }

    private static K8sClusterRef resolveKindRef() throws Exception {
        List<KubeConfigLoader.DiscoveredContext> all = KubeConfigLoader.discoverAll();
        KubeConfigLoader.DiscoveredContext kind = all.stream()
                .filter(c -> c.summary().contextName().contains("kind"))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "no kind context found — install-operators.sh prerequisite."));
        return new K8sClusterRef(
                1L, "kind-soak",
                kind.sourcePath().toAbsolutePath().toString(),
                kind.summary().contextName(),
                kind.summary().namespace(),
                kind.summary().serverUrl(),
                0L, Optional.empty());
    }

    private static Duration parseBudget(String raw) {
        if (raw == null || raw.isBlank()) return null;
        String s = raw.trim().toLowerCase();
        try {
            if (s.endsWith("h")) return Duration.ofHours(
                    Long.parseLong(s.substring(0, s.length() - 1)));
            if (s.endsWith("m")) return Duration.ofMinutes(
                    Long.parseLong(s.substring(0, s.length() - 1)));
            if (s.endsWith("s")) return Duration.ofSeconds(
                    Long.parseLong(s.substring(0, s.length() - 1)));
            return Duration.ofHours(Long.parseLong(s));
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException(
                    "MEX_SOAK must be a duration like '72h' or '4500m', got: " + raw);
        }
    }

    private static int parseInt(String raw, int fallback) {
        if (raw == null || raw.isBlank()) return fallback;
        try { return Integer.parseInt(raw.trim()); }
        catch (NumberFormatException nfe) { return fallback; }
    }
}
