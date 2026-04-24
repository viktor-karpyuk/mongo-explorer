package com.kubrik.mex.labs.k8s.distro;

import com.kubrik.mex.k8s.cluster.KubeClusterService;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.labs.k8s.model.LabK8sCluster;
import com.kubrik.mex.labs.k8s.model.LabK8sClusterStatus;
import com.kubrik.mex.labs.k8s.model.LabK8sDistro;
import com.kubrik.mex.labs.k8s.store.LabK8sClusterDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8-N2 — Distro-lifecycle shim called by
 * {@code LabK8sLifecycleService} to bring a local cluster up before
 * the production provisioning pipeline runs against it.
 *
 * <p>Decision 11 anchor: this service is the ONLY new Lab-specific
 * code on the cluster-lifecycle axis. Once a cluster is RUNNING it
 * registers with {@link KubeClusterService} and the rest of the
 * v2.8.1 pipeline ({@code PROV-*} / {@code OP-*} / {@code PRE-*} /
 * {@code ROLL-*} / {@code TEAR-*}) handles it identically to a
 * production cluster.</p>
 *
 * <p>Default wall budgets:</p>
 * <ul>
 *   <li>Create — 8 min (cold-pull of k8s images on slow networks).</li>
 *   <li>Start — 2 min (restart an existing distro profile).</li>
 *   <li>Stop / Delete — 90 s (distros stop cleanly in seconds).</li>
 *   <li>Status — 10 s.</li>
 * </ul>
 */
public final class LocalK8sDistroService {

    private static final Logger log = LoggerFactory.getLogger(LocalK8sDistroService.class);

    public static final int CREATE_BUDGET_SECONDS = 8 * 60;
    public static final int START_BUDGET_SECONDS = 120;
    public static final int STOP_BUDGET_SECONDS = 90;
    public static final int DELETE_BUDGET_SECONDS = 90;
    public static final int STATUS_BUDGET_SECONDS = 10;

    private final DistroDetector detector;
    private final LabK8sClusterDao clusterDao;
    private final KubeClusterService kubeClusterService;

    public LocalK8sDistroService(DistroDetector detector,
                                   LabK8sClusterDao clusterDao,
                                   KubeClusterService kubeClusterService) {
        this.detector = Objects.requireNonNull(detector, "detector");
        this.clusterDao = Objects.requireNonNull(clusterDao, "clusterDao");
        this.kubeClusterService = Objects.requireNonNull(kubeClusterService, "kubeClusterService");
    }

    /**
     * Create a new local cluster, then register it with
     * {@link KubeClusterService} so the production pipeline sees a
     * {@link K8sClusterRef}. Idempotent on an existing identifier —
     * falls through to a re-start path.
     */
    public Result createOrStart(LabK8sDistro distro, String identifier) {
        DistroAdapter adapter = detector.adapter(distro);
        if (detector.detect(distro).isEmpty()) {
            return Result.failed(distro + " CLI not on PATH; "
                    + "install " + distro.cliName() + " or pick another distro.");
        }

        // 1. Insert the row (CREATING) so lifecycle state survives a crash mid-create.
        long rowId;
        try {
            Optional<LabK8sCluster> existing = clusterDao.findByIdentifier(distro, identifier);
            if (existing.isPresent()) {
                LabK8sCluster row = existing.get();
                if (row.status() == LabK8sClusterStatus.RUNNING) {
                    return Result.already(row);
                }
                rowId = row.id();
            } else {
                rowId = clusterDao.insertCreating(distro, identifier,
                        adapter.contextFor(identifier),
                        adapter.kubeconfigPath().toString());
            }
        } catch (SQLException sqle) {
            return Result.failed("DB row insert failed: " + sqle.getMessage());
        }

        // 2. Run the CLI create.
        CliRunner.CliResult created;
        try {
            created = adapter.create(identifier, CREATE_BUDGET_SECONDS);
        } catch (IOException ioe) {
            markStatus(rowId, LabK8sClusterStatus.FAILED, "destroyed_at");
            return Result.failed(distro.cliName() + " create failed: " + ioe.getMessage());
        }
        if (!created.ok()) {
            markStatus(rowId, LabK8sClusterStatus.FAILED, "destroyed_at");
            return Result.failed(distro.cliName() + " create returned "
                    + created.exitCode() + ": " + created.stderrTail(10));
        }

        // 3. Mark RUNNING + attach to the production pipeline by
        //    registering the kubeconfig context as a K8sClusterRef.
        long now = System.currentTimeMillis();
        try {
            clusterDao.updateStatus(rowId, LabK8sClusterStatus.RUNNING, now, "last_started_at");
        } catch (SQLException ignored) {}

        K8sClusterRef ref;
        try {
            ref = kubeClusterService.add(
                    "lab:" + distro.cliName() + "/" + identifier,
                    adapter.kubeconfigPath().toString(),
                    adapter.contextFor(identifier),
                    Optional.of("default"),
                    Optional.empty());
            clusterDao.attachK8sCluster(rowId, ref.id());
        } catch (SQLException sqle) {
            // Cluster is up but the registration failed — the user can
            // re-register manually from the Clusters pane. Return
            // success but note the warning.
            log.warn("lab k8s cluster is up but kube-cluster registration failed: {}",
                    sqle.getMessage());
            return Result.created(loadOrThrow(rowId), null);
        }

        return Result.created(loadOrThrow(rowId), ref);
    }

    public Result stop(long clusterId) {
        LabK8sCluster row = loadOrThrow(clusterId);
        DistroAdapter adapter = detector.adapter(row.distro());
        CliRunner.CliResult r;
        try {
            r = adapter.stop(row.identifier(), STOP_BUDGET_SECONDS);
        } catch (IOException ioe) {
            return Result.failed(row.distro().cliName() + " stop: " + ioe.getMessage());
        }
        if (!r.ok()) {
            return Result.failed(row.distro().cliName() + " stop returned "
                    + r.exitCode() + ": " + r.stderrTail(8));
        }
        long now = System.currentTimeMillis();
        markStatus(clusterId, LabK8sClusterStatus.STOPPED, "last_stopped_at", now);
        return Result.ok(loadOrThrow(clusterId));
    }

    public Result restart(long clusterId) {
        LabK8sCluster row = loadOrThrow(clusterId);
        DistroAdapter adapter = detector.adapter(row.distro());
        CliRunner.CliResult r;
        try {
            r = adapter.start(row.identifier(), START_BUDGET_SECONDS);
        } catch (IOException ioe) {
            return Result.failed(row.distro().cliName() + " start: " + ioe.getMessage());
        }
        if (!r.ok()) {
            return Result.failed(row.distro().cliName() + " start returned "
                    + r.exitCode() + ": " + r.stderrTail(8));
        }
        long now = System.currentTimeMillis();
        markStatus(clusterId, LabK8sClusterStatus.RUNNING, "last_started_at", now);
        return Result.ok(loadOrThrow(clusterId));
    }

    public Result destroy(long clusterId) {
        LabK8sCluster row = loadOrThrow(clusterId);
        DistroAdapter adapter = detector.adapter(row.distro());
        CliRunner.CliResult r = null;
        try {
            r = adapter.delete(row.identifier(), DELETE_BUDGET_SECONDS);
        } catch (IOException ioe) {
            // Even if the CLI threw, flip the row to DESTROYED — the
            // intent is clear and leaving it RUNNING is worse than a
            // dangling tombstone.
            log.warn("distro delete threw — flipping DESTROYED anyway: {}", ioe.getMessage());
        }
        if (r != null && !r.ok()) {
            log.warn("distro delete returned {} — flipping DESTROYED anyway: {}",
                    r.exitCode(), r.stderrTail(8));
        }
        // Forget the K8sClusterRef too — leaving it around means the
        // Clusters pane has a row pointing at a dead cluster.
        row.k8sClusterId().ifPresent(k8sId -> {
            try { kubeClusterService.remove(k8sId); }
            catch (Exception e) {
                log.debug("remove k8s cluster row {}: {}", k8sId, e.toString());
            }
        });
        long now = System.currentTimeMillis();
        markStatus(clusterId, LabK8sClusterStatus.DESTROYED, "destroyed_at", now);
        return Result.ok(loadOrThrow(clusterId));
    }

    /* ============================ helpers ============================ */

    private LabK8sCluster loadOrThrow(long id) {
        try {
            return clusterDao.findById(id)
                    .orElseThrow(() -> new IllegalStateException("row " + id + " vanished"));
        } catch (SQLException sqle) {
            throw new RuntimeException(sqle);
        }
    }

    private void markStatus(long id, LabK8sClusterStatus status, String col) {
        markStatus(id, status, col, System.currentTimeMillis());
    }

    private void markStatus(long id, LabK8sClusterStatus status, String col, long at) {
        try { clusterDao.updateStatus(id, status, at, col); }
        catch (SQLException sqle) { log.warn("updateStatus {} {}: {}", id, status, sqle.toString()); }
    }

    /** Seal-ish result surface shared by every lifecycle call. */
    public sealed interface Result {
        record Ok(LabK8sCluster cluster) implements Result {}
        record Created(LabK8sCluster cluster, K8sClusterRef ref) implements Result {}
        record Already(LabK8sCluster cluster) implements Result {}
        record Failed(String reason) implements Result {}

        static Result ok(LabK8sCluster c) { return new Ok(c); }
        static Result created(LabK8sCluster c, K8sClusterRef ref) { return new Created(c, ref); }
        static Result already(LabK8sCluster c) { return new Already(c); }
        static Result failed(String why) { return new Failed(why); }

        default boolean success() {
            return this instanceof Ok || this instanceof Created || this instanceof Already;
        }
    }
}
