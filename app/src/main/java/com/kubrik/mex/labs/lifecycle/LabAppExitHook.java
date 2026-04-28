package com.kubrik.mex.labs.lifecycle;

import com.kubrik.mex.labs.model.LabDeployment;
import com.kubrik.mex.labs.model.LabStatus;
import com.kubrik.mex.labs.store.LabDeploymentDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * v2.8.4 LAB-LIFECYCLE-7 — JVM shutdown hook that honours the
 * {@code labs.on_exit} setting:
 *
 * <ul>
 *   <li>{@code stop} (default) → {@code composeStop} on every
 *       RUNNING row, bounded by a 15s wall.</li>
 *   <li>{@code leave_running} → no-op (user's choice to keep Labs
 *       up across app restarts).</li>
 *   <li>{@code destroy} → {@code composeDown -v} on every
 *       non-DESTROYED row, bounded by 30s.</li>
 * </ul>
 *
 * <p>Wall-bounded so shutdown can't drag. On timeout we log +
 * surface a best-effort warning next launch via an adoption banner
 * (when reconciler finds orphans); we never block the JVM exit
 * waiting for Docker.</p>
 */
public final class LabAppExitHook {

    private static final Logger log = LoggerFactory.getLogger(LabAppExitHook.class);

    public enum Policy { STOP, LEAVE_RUNNING, DESTROY }

    private final LabLifecycleService lifecycle;
    private final LabDeploymentDao deploymentDao;
    private final Policy policy;

    public LabAppExitHook(LabLifecycleService lifecycle,
                          LabDeploymentDao deploymentDao,
                          Policy policy) {
        this.lifecycle = lifecycle;
        this.deploymentDao = deploymentDao;
        this.policy = policy == null ? Policy.STOP : policy;
    }

    /** Register with the JVM. Idempotent — safe to call twice if a
     *  reinit happens. */
    public void register() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::runOnce,
                "mex-labs-exit-hook"));
    }

    /** Test-visible form. Walks live rows + applies the policy. */
    void runOnce() {
        if (policy == Policy.LEAVE_RUNNING) {
            log.info("labs.on_exit=leave_running — keeping Labs up");
            return;
        }
        List<LabDeployment> live = deploymentDao.listLive();
        if (live.isEmpty()) return;

        Duration budget = policy == Policy.DESTROY
                ? Duration.ofSeconds(30) : Duration.ofSeconds(15);

        // Parallel shutdown — one Lab's slow compose stop shouldn't
        // starve the others. Each transition is bounded by its own
        // DockerClient.COMPOSE_*_TIMEOUT so virtual threads can't
        // outlive the shutdown budget.
        var ex = Executors.newVirtualThreadPerTaskExecutor();
        try {
            for (LabDeployment lab : live) {
                ex.submit(() -> {
                    try {
                        if (policy == Policy.STOP
                                && lab.status() == LabStatus.RUNNING) {
                            lifecycle.stop(lab.id());
                        } else if (policy == Policy.DESTROY
                                && lab.status() != LabStatus.DESTROYED) {
                            lifecycle.destroy(lab.id());
                        }
                    } catch (Exception e) {
                        log.warn("exit hook failed on lab {}: {}",
                                lab.id(), e.getMessage());
                    }
                });
            }
            ex.shutdown();
            boolean done = ex.awaitTermination(budget.toMillis(),
                    TimeUnit.MILLISECONDS);
            if (!done) {
                log.warn("labs exit hook exceeded {}s — orphan compose "
                        + "projects may remain; next launch's reconciler "
                        + "will clean up", budget.toSeconds());
                ex.shutdownNow();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            ex.shutdownNow();
        }
    }

    public static Policy parsePolicy(String setting) {
        if (setting == null) return Policy.STOP;
        return switch (setting.trim().toLowerCase()) {
            case "leave_running" -> Policy.LEAVE_RUNNING;
            case "destroy" -> Policy.DESTROY;
            default -> Policy.STOP;
        };
    }
}
