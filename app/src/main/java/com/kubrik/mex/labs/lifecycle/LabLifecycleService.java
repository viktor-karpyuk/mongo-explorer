package com.kubrik.mex.labs.lifecycle;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.labs.docker.DockerClient;
import com.kubrik.mex.labs.docker.ExecResult;
import com.kubrik.mex.labs.events.LabLifecycleEvent;
import com.kubrik.mex.labs.model.LabDeployment;
import com.kubrik.mex.labs.model.LabEvent;
import com.kubrik.mex.labs.model.LabStatus;
import com.kubrik.mex.labs.model.LabTemplate;
import com.kubrik.mex.labs.model.PortMap;
import com.kubrik.mex.labs.ports.EphemeralPortAllocator;
import com.kubrik.mex.labs.store.LabDeploymentDao;
import com.kubrik.mex.labs.store.LabEventDao;
import com.kubrik.mex.labs.templates.ComposeRenderer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * v2.8.4 Q2.8.4-C — Orchestrates Lab creation and lifecycle.
 *
 * <p>Apply flow (the interesting path):</p>
 * <ol>
 *   <li>Allocate ports for every container in the template.</li>
 *   <li>Render the compose file to
 *       {@code <app_data>/labs/<project>/docker-compose.yml}.</li>
 *   <li>Insert a {@code lab_deployments} row in {@code CREATING}
 *       state + emit an {@code APPLY} event.</li>
 *   <li>{@code docker compose up -d --wait} on a virtual thread.</li>
 *   <li>{@link LabHealthWatcher} polls the entry container's
 *       ping until it returns ok.</li>
 *   <li>{@link LabAutoConnectionWriter} inserts the
 *       origin=LAB connection row + back-pointer.</li>
 *   <li>Flip status to {@code RUNNING} + emit {@code HEALTHY}.</li>
 * </ol>
 *
 * <p>Any step's failure flips status to {@code FAILED}, runs a
 * best-effort {@code docker compose down -v} cleanup, and emits a
 * {@code FAILED} event. The UI rollout viewer stays attached
 * throughout so the user sees the whole transition live.</p>
 *
 * <p>Seeding is out-of-scope for Q2.8.4-C — the stub here calls a
 * no-op {@link SeedStep}; Q2.8.4-E plugs in the real SeedRunner.</p>
 */
public final class LabLifecycleService {

    private static final Logger log = LoggerFactory.getLogger(LabLifecycleService.class);

    private final DockerClient docker;
    private final ComposeRenderer renderer;
    private final EphemeralPortAllocator ports;
    private final LabHealthWatcher health;
    private final LabAutoConnectionWriter connectionWriter;
    private final LabDeploymentDao deploymentDao;
    private final LabEventDao eventDao;
    private final EventBus eventBus;
    private final Path dataDir;
    private final String mexVersion;
    /** Seeding hook — Q2.8.4-E replaces the default no-op. */
    private SeedStep seedStep = (lab, template) -> { /* no-op */ };

    @FunctionalInterface
    public interface SeedStep {
        void run(LabDeployment lab, LabTemplate template);
    }

    public LabLifecycleService(DockerClient docker,
                                ComposeRenderer renderer,
                                EphemeralPortAllocator ports,
                                LabHealthWatcher health,
                                LabAutoConnectionWriter connectionWriter,
                                LabDeploymentDao deploymentDao,
                                LabEventDao eventDao,
                                EventBus eventBus,
                                Path dataDir,
                                String mexVersion) {
        this.docker = docker;
        this.renderer = renderer;
        this.ports = ports;
        this.health = health;
        this.connectionWriter = connectionWriter;
        this.deploymentDao = deploymentDao;
        this.eventDao = eventDao;
        this.eventBus = eventBus;
        this.dataDir = dataDir;
        this.mexVersion = mexVersion;
    }

    /** Hot-swap the seed step — Q2.8.4-E registers the real runner
     *  once it's available. */
    public void setSeedStep(SeedStep step) {
        if (step != null) this.seedStep = step;
    }

    /** Apply a template — returns a future that resolves when the
     *  Lab is either RUNNING or FAILED. The UI attaches to the
     *  returned future for status + to the event stream for progress
     *  lines. */
    public CompletableFuture<LabDeployment> apply(LabTemplate template,
                                                   ApplyOptions opts,
                                                   Consumer<String> progress) {
        CompletableFuture<LabDeployment> future = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try {
                future.complete(applyBlocking(template, opts, progress));
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    /** Test-visible form — runs on the caller's thread. */
    LabDeployment applyBlocking(LabTemplate template, ApplyOptions opts,
                                 Consumer<String> progress) throws IOException {
        String project = composeProjectFor(template.id());
        notify(progress, "Allocating ephemeral ports…");
        PortMap allocatedPorts = ports.allocate(template.containerNames());

        notify(progress, "Rendering compose file…");
        String mongoTag = opts.mongoTag() == null
                ? template.defaultMongoTag() : opts.mongoTag();
        Path composePath = renderer.render(template, allocatedPorts, mongoTag,
                opts.authEnabled(), project, dataDir);

        LabDeployment row = deploymentDao.insert(new LabDeployment(
                -1, template.id(), mexVersion,
                opts.displayName() == null
                        ? template.displayName() + " (Lab " + shortSuffix(project) + ")"
                        : opts.displayName(),
                project, composePath.toString(),
                allocatedPorts, LabStatus.CREATING,
                opts.keepDataOnStop(), opts.authEnabled(),
                System.currentTimeMillis(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                mongoTag, Optional.empty()));
        emit(row.id(), LabStatus.CREATING, LabEvent.Kind.APPLY,
                "starting " + project);

        Path logDir = composePath.getParent();
        try {
            notify(progress, "docker compose up -d (this can take a minute)…");
            ExecResult up = docker.composeUp(composePath, project,
                    logDir.resolve("compose-up.stdout.log"),
                    logDir.resolve("compose-up.stderr.log"));
            if (!up.ok()) {
                return fail(row, "compose up failed: " + up.combinedTail(),
                        progress);
            }

            notify(progress, "Waiting for mongod to answer ping…");
            if (!health.awaitHealthy(row, template)) {
                return fail(row, "health probe timed out after "
                        + template.estStartupSeconds() * 2 + "s",
                        progress);
            }

            notify(progress, "Seeding sample data (if any)…");
            try {
                seedStep.run(row, template);
            } catch (Exception seedErr) {
                // Seeding is best-effort for Q2.8.4-C — flag but don't
                // fail the Lab outright; user can re-seed manually.
                emit(row.id(), LabStatus.CREATING, LabEvent.Kind.SEED_FAILED,
                        seedErr.getMessage());
            }

            notify(progress, "Creating Mongo Explorer connection…");
            String connectionId = connectionWriter.write(row, template);
            deploymentDao.setConnectionId(row.id(), connectionId);

            long now = System.currentTimeMillis();
            deploymentDao.updateStatus(row.id(), LabStatus.RUNNING,
                    now, "last_started_at");
            emit(row.id(), LabStatus.RUNNING, LabEvent.Kind.HEALTHY,
                    "connection " + connectionId);
            notify(progress, "Lab is running.");
            return deploymentDao.byId(row.id()).orElseThrow();

        } catch (Throwable t) {
            log.warn("Lab apply failed", t);
            return fail(row, t.getClass().getSimpleName()
                    + ": " + t.getMessage(), progress);
        }
    }

    private LabDeployment fail(LabDeployment row, String message,
                                 Consumer<String> progress) {
        emit(row.id(), LabStatus.FAILED, LabEvent.Kind.FAILED, message);
        notify(progress, "FAILED: " + message);
        deploymentDao.updateStatus(row.id(), LabStatus.FAILED,
                System.currentTimeMillis(), "destroyed_at");
        // Best-effort cleanup so the user isn't left with dangling
        // containers on a failed apply.
        try {
            Path logDir = Path.of(row.composeFilePath()).getParent();
            Files.createDirectories(logDir);
            docker.composeDown(row.composeProject(), /*removeVolumes=*/true,
                    logDir.resolve("compose-down-cleanup.stdout.log"),
                    logDir.resolve("compose-down-cleanup.stderr.log"));
        } catch (Exception cleanupErr) {
            log.warn("cleanup after failed apply also failed: {}",
                    cleanupErr.getMessage());
        }
        return deploymentDao.byId(row.id()).orElse(row);
    }

    /* ============================== helpers ============================== */

    private void emit(long labId, LabStatus status, LabEvent.Kind kind,
                       String message) {
        long now = System.currentTimeMillis();
        eventDao.insert(labId, kind, now, message);
        if (eventBus != null) {
            eventBus.publish(new LabLifecycleEvent(labId, status, kind,
                    message, now));
        }
    }

    private static void notify(Consumer<String> progress, String msg) {
        if (progress != null) progress.accept(msg);
        log.info("[lab] {}", msg);
    }

    /** {@code mex-lab-<template-id>-<8-char-uuid>}. */
    static String composeProjectFor(String templateId) {
        String suffix = UUID.randomUUID().toString().replace("-", "")
                .substring(0, 8);
        return "mex-lab-" + templateId + "-" + suffix;
    }

    private static String shortSuffix(String project) {
        return project.length() >= 8
                ? project.substring(project.length() - 8) : project;
    }

    /** Apply options surfaced by the UI wizard. */
    public record ApplyOptions(
            String displayName,      // nullable — defaults to template display_name
            String mongoTag,          // nullable — defaults to template default_mongo_tag
            boolean authEnabled,
            boolean keepDataOnStop
    ) {
        public static ApplyOptions defaults() {
            return new ApplyOptions(null, null, false, false);
        }
    }

    /* ========================= Stop / Start / Destroy ========================= */

    public sealed interface TransitionResult {
        record Ok(LabDeployment lab) implements TransitionResult {}
        record Rejected(String reason) implements TransitionResult {}
        record Failed(String reason) implements TransitionResult {}
    }

    /** Pauses containers but keeps volumes. Guards on status=RUNNING. */
    public TransitionResult stop(long labId) {
        LabDeployment lab = deploymentDao.byId(labId).orElse(null);
        if (lab == null) return new TransitionResult.Rejected("unknown lab " + labId);
        if (lab.status() != LabStatus.RUNNING) {
            return new TransitionResult.Rejected(
                    "expected RUNNING, was " + lab.status());
        }
        Path logDir = Path.of(lab.composeFilePath()).getParent();
        try {
            ExecResult r = docker.composeStop(lab.composeProject(),
                    logDir.resolve("compose-stop.stdout.log"),
                    logDir.resolve("compose-stop.stderr.log"));
            if (!r.ok()) {
                emit(lab.id(), LabStatus.RUNNING, LabEvent.Kind.FAILED,
                        "stop failed: " + r.combinedTail());
                return new TransitionResult.Failed(
                        "compose stop failed: " + r.combinedTail());
            }
        } catch (IOException ioe) {
            emit(lab.id(), LabStatus.RUNNING, LabEvent.Kind.FAILED,
                    ioe.getMessage());
            return new TransitionResult.Failed(ioe.getMessage());
        }
        long now = System.currentTimeMillis();
        deploymentDao.updateStatus(lab.id(), LabStatus.STOPPED, now,
                "last_stopped_at");
        emit(lab.id(), LabStatus.STOPPED, LabEvent.Kind.STOP, null);
        return new TransitionResult.Ok(deploymentDao.byId(lab.id()).orElseThrow());
    }

    /** Resumes a stopped Lab. Guards on status=STOPPED. */
    public TransitionResult start(long labId) {
        LabDeployment lab = deploymentDao.byId(labId).orElse(null);
        if (lab == null) return new TransitionResult.Rejected("unknown lab " + labId);
        if (lab.status() != LabStatus.STOPPED) {
            return new TransitionResult.Rejected(
                    "expected STOPPED, was " + lab.status());
        }
        Path logDir = Path.of(lab.composeFilePath()).getParent();
        try {
            ExecResult r = docker.composeStart(lab.composeProject(),
                    logDir.resolve("compose-start.stdout.log"),
                    logDir.resolve("compose-start.stderr.log"));
            if (!r.ok()) {
                return new TransitionResult.Failed(
                        "compose start failed: " + r.combinedTail());
            }
        } catch (IOException ioe) {
            return new TransitionResult.Failed(ioe.getMessage());
        }
        long now = System.currentTimeMillis();
        deploymentDao.updateStatus(lab.id(), LabStatus.RUNNING, now,
                "last_started_at");
        emit(lab.id(), LabStatus.RUNNING, LabEvent.Kind.START, null);
        return new TransitionResult.Ok(deploymentDao.byId(lab.id()).orElseThrow());
    }

    /** Removes containers + networks + volumes. Flips the row to
     *  DESTROYED (tombstone) and deletes the auto-created connection.
     *  Permitted from any non-DESTROYED state so a stuck CREATING or
     *  FAILED row can still be cleaned up. */
    public TransitionResult destroy(long labId) {
        LabDeployment lab = deploymentDao.byId(labId).orElse(null);
        if (lab == null) return new TransitionResult.Rejected("unknown lab " + labId);
        if (lab.status() == LabStatus.DESTROYED) {
            return new TransitionResult.Rejected("already destroyed");
        }
        Path logDir = Path.of(lab.composeFilePath()).getParent();
        try {
            ExecResult r = docker.composeDown(lab.composeProject(),
                    /*removeVolumes=*/true,
                    logDir.resolve("compose-destroy.stdout.log"),
                    logDir.resolve("compose-destroy.stderr.log"));
            if (!r.ok()) {
                // `compose down` on an already-down project returns 0
                // normally. Non-zero here means the daemon is down or
                // the project is half-gone; we still flip the row to
                // DESTROYED because the intent is clear.
                log.warn("destroy: compose down returned {} — flipping DESTROYED anyway ({})",
                        r.exitCode(), r.combinedTail());
            }
        } catch (IOException ioe) {
            log.warn("destroy: compose down threw {} — flipping DESTROYED anyway",
                    ioe.getMessage());
        }
        // Delete the auto-created connection so it doesn't linger in
        // the user's tree pointing at a vanished Lab.
        lab.connectionId().ifPresent(cxId -> {
            try { connectionWriter.deleteLabOriginConnection(cxId); }
            catch (Exception cleanupErr) {
                log.warn("failed to delete lab-origin connection {}: {}",
                        cxId, cleanupErr.getMessage());
            }
        });

        long now = System.currentTimeMillis();
        deploymentDao.updateStatus(lab.id(), LabStatus.DESTROYED, now,
                "destroyed_at");
        emit(lab.id(), LabStatus.DESTROYED, LabEvent.Kind.DESTROY, null);
        return new TransitionResult.Ok(deploymentDao.byId(lab.id()).orElseThrow());
    }
}
