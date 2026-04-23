package com.kubrik.mex.labs.lifecycle;

import com.kubrik.mex.labs.docker.DockerClient;
import com.kubrik.mex.labs.docker.ExecResult;
import com.kubrik.mex.labs.model.LabDeployment;
import com.kubrik.mex.labs.model.LabEvent;
import com.kubrik.mex.labs.model.LabStatus;
import com.kubrik.mex.labs.store.LabDeploymentDao;
import com.kubrik.mex.labs.store.LabEventDao;
import com.kubrik.mex.events.EventBus;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * v2.8.4 LAB-LIFECYCLE-5 — App-start reconciliation between the
 * {@code lab_deployments} table and the live {@code docker compose ls}
 * output. Catches three drift modes:
 *
 * <ul>
 *   <li>DB row RUNNING but compose project missing → flip to FAILED
 *       with reason {@code compose-project-missing}.</li>
 *   <li>DB row RUNNING but compose project stopped → flip to STOPPED
 *       (user stopped Docker Desktop while the app was closed).</li>
 *   <li>Compose project named {@code mex-lab-*} exists with no DB
 *       row (user reinstalled the app, or the SQLite file was moved)
 *       → NOT adopted in Q2.8.4-D. Adoption is tracked as a future
 *       polish item; for now we log and skip.</li>
 * </ul>
 *
 * <p>Runs once at app start on a background thread so startup stays
 * fast even if Docker isn't responding.</p>
 */
public final class LabReconciler {

    private static final Logger log = LoggerFactory.getLogger(LabReconciler.class);

    private final DockerClient docker;
    private final LabDeploymentDao deploymentDao;
    private final LabEventDao eventDao;
    private final EventBus eventBus;
    private final Path dataDir;

    public LabReconciler(DockerClient docker,
                          LabDeploymentDao deploymentDao,
                          LabEventDao eventDao,
                          EventBus eventBus,
                          Path dataDir) {
        this.docker = docker;
        this.deploymentDao = deploymentDao;
        this.eventDao = eventDao;
        this.eventBus = eventBus;
        this.dataDir = dataDir;
    }

    /** Kick off a one-shot reconcile on a virtual thread. The caller
     *  doesn't need to wait. */
    public void reconcileAsync() {
        Thread.startVirtualThread(() -> {
            try { reconcileBlocking(); }
            catch (Exception e) {
                log.warn("lab reconcile failed: {}", e.getMessage());
            }
        });
    }

    /** Test-visible form. */
    void reconcileBlocking() throws IOException {
        List<LabDeployment> rows = deploymentDao.listLive();
        if (rows.isEmpty()) return;

        Map<String, ComposeStatus> live = composeProjectStatuses();
        Set<String> seen = new HashSet<>();

        for (LabDeployment row : rows) {
            String project = row.composeProject();
            seen.add(project);
            ComposeStatus cs = live.get(project);
            if (cs == null) {
                if (row.status() == LabStatus.RUNNING
                        || row.status() == LabStatus.STOPPED
                        || row.status() == LabStatus.CREATING) {
                    log.info("reconcile: compose project {} missing — flipping {} → FAILED",
                            project, row.status());
                    deploymentDao.updateStatus(row.id(), LabStatus.FAILED,
                            System.currentTimeMillis(), null);
                    eventDao.insert(row.id(), LabEvent.Kind.FAILED,
                            System.currentTimeMillis(),
                            "reconcile: compose-project-missing");
                }
                continue;
            }
            if (cs == ComposeStatus.RUNNING && row.status() != LabStatus.RUNNING) {
                log.info("reconcile: project {} running, row {} — flipping to RUNNING",
                        project, row.status());
                deploymentDao.updateStatus(row.id(), LabStatus.RUNNING,
                        System.currentTimeMillis(), "last_started_at");
            } else if (cs == ComposeStatus.STOPPED && row.status() == LabStatus.RUNNING) {
                log.info("reconcile: project {} stopped outside app — flipping row to STOPPED",
                        project);
                deploymentDao.updateStatus(row.id(), LabStatus.STOPPED,
                        System.currentTimeMillis(), "last_stopped_at");
            }
        }

        // Orphan adoption — log unmatched mex-lab-* projects but
        // don't auto-adopt (milestone §3.7 says ADOPTED event + UI
        // banner, but the banner UI lands in Q2.8.4-F; the log
        // gives the user a breadcrumb).
        for (String project : live.keySet()) {
            if (!seen.contains(project)) {
                log.info("reconcile: orphan project {} has no DB row; "
                        + "left untouched. A future release may offer "
                        + "adoption.", project);
            }
        }
    }

    enum ComposeStatus { RUNNING, STOPPED, UNKNOWN }

    /** Run {@code docker compose ls --format json} and return a map
     *  from project name to state. Only considers projects whose
     *  name starts with {@code mex-lab-} so other tenants of the
     *  Docker daemon aren't touched. */
    @SuppressWarnings("unchecked")
    private Map<String, ComposeStatus> composeProjectStatuses() throws IOException {
        Files.createDirectories(dataDir);
        ExecResult r = docker.composeLs(
                dataDir.resolve("compose-ls.stdout.log"),
                dataDir.resolve("compose-ls.stderr.log"));
        if (!r.ok()) {
            log.debug("compose ls failed: {}", r.combinedTail());
            return Map.of();
        }
        String body = r.tailStdout() == null ? "" : r.tailStdout().trim();
        if (body.isEmpty()) return Map.of();
        // Output is a JSON array when --format json is passed.
        if (!body.startsWith("[")) {
            log.debug("compose ls emitted non-array JSON: {}", body);
            return Map.of();
        }
        Map<String, ComposeStatus> out = new HashMap<>();
        // BSON can parse single-line JSON arrays via Document.parse
        // applied to a { "x": <array> } wrapper.
        Document wrapped;
        try {
            wrapped = Document.parse("{\"x\":" + body + "}");
        } catch (Exception e) {
            log.debug("compose ls JSON parse failed: {}", e.getMessage());
            return Map.of();
        }
        List<Document> entries = (List<Document>) wrapped.get("x", List.class);
        if (entries == null) return out;
        for (Document d : entries) {
            String name = d.getString("Name");
            String status = d.getString("Status");
            if (name == null || !name.startsWith("mex-lab-")) continue;
            out.put(name, parseStatus(status));
        }
        return out;
    }

    static ComposeStatus parseStatus(String status) {
        if (status == null) return ComposeStatus.UNKNOWN;
        String lower = status.toLowerCase();
        if (lower.contains("running")) return ComposeStatus.RUNNING;
        if (lower.contains("exited") || lower.contains("created")
                || lower.contains("stopped")) return ComposeStatus.STOPPED;
        return ComposeStatus.UNKNOWN;
    }
}
