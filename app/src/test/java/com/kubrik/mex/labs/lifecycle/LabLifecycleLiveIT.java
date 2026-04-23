package com.kubrik.mex.labs.lifecycle;

import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.labs.docker.DockerClient;
import com.kubrik.mex.labs.model.EngineStatus;
import com.kubrik.mex.labs.model.LabDeployment;
import com.kubrik.mex.labs.model.LabStatus;
import com.kubrik.mex.labs.model.LabTemplate;
import com.kubrik.mex.labs.ports.EphemeralPortAllocator;
import com.kubrik.mex.labs.store.LabDeploymentDao;
import com.kubrik.mex.labs.store.LabEventDao;
import com.kubrik.mex.labs.templates.ComposeRenderer;
import com.kubrik.mex.labs.templates.LabTemplateRegistry;
import com.kubrik.mex.store.ConnectionStore;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.4 Q2.8.4-G — Live-Docker IT for the full Lab lifecycle:
 * Apply → Stop → Start → Destroy against the standalone template.
 *
 * <p>Tagged {@code labDocker} so it's skipped from the default
 * build. Run explicitly with {@code ./gradlew :app:labDockerTest};
 * requires Docker + {@code docker compose} on PATH.</p>
 *
 * <p>Only the standalone template is exercised here — larger
 * topologies (rs-3, sharded-1, triple-rs) need extra wall time and
 * are covered by the cross-platform smoke matrix.</p>
 */
@Tag("labDocker")
class LabLifecycleLiveIT {

    @TempDir Path dataDir;

    private Database db;
    private DockerClient docker;
    private LabLifecycleService lifecycle;
    private LabDeploymentDao deploymentDao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        docker = new DockerClient();
        EngineStatus status = docker.status();
        Assumptions.assumeTrue(status == EngineStatus.READY,
                "live IT needs a healthy Docker runtime; status=" + status);

        db = new Database();
        deploymentDao = new LabDeploymentDao(db);
        LabEventDao eventDao = new LabEventDao(db);
        ConnectionStore connectionStore = new ConnectionStore(db);
        EventBus bus = new EventBus();

        LabTemplateRegistry registry = new LabTemplateRegistry();
        registry.loadBuiltins();

        lifecycle = new LabLifecycleService(
                docker,
                new ComposeRenderer(),
                new EphemeralPortAllocator(),
                new LabHealthWatcher(),
                new LabAutoConnectionWriter(connectionStore),
                deploymentDao, eventDao, bus,
                dataDir.resolve("labs"),
                "it");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void full_lifecycle_standalone() throws Exception {
        LabTemplateRegistry r = new LabTemplateRegistry();
        r.loadBuiltins();
        LabTemplate standalone = r.byId("standalone").orElseThrow();

        Consumer<String> progress = line ->
                System.out.println("[lifecycle] " + line);

        // Apply
        LabDeployment applied = lifecycle.applyBlocking(standalone,
                LabLifecycleService.ApplyOptions.defaults(), progress);
        try {
            assertEquals(LabStatus.RUNNING, applied.status(),
                    "apply should reach RUNNING; got " + applied.status());
            assertTrue(applied.connectionId().isPresent(),
                    "auto-connection row should exist");

            // Stop
            LabLifecycleService.TransitionResult stopR = lifecycle.stop(applied.id());
            assertInstanceOf(LabLifecycleService.TransitionResult.Ok.class, stopR);
            assertEquals(LabStatus.STOPPED,
                    deploymentDao.byId(applied.id()).orElseThrow().status());

            // Stop from stopped = rejected
            LabLifecycleService.TransitionResult dupStop = lifecycle.stop(applied.id());
            assertInstanceOf(LabLifecycleService.TransitionResult.Rejected.class, dupStop);

            // Start
            LabLifecycleService.TransitionResult startR = lifecycle.start(applied.id());
            assertInstanceOf(LabLifecycleService.TransitionResult.Ok.class, startR);
            assertEquals(LabStatus.RUNNING,
                    deploymentDao.byId(applied.id()).orElseThrow().status());
        } finally {
            // Destroy regardless of test outcome so the daemon isn't
            // left with a running lab container between IT runs.
            lifecycle.destroy(applied.id());
            assertEquals(LabStatus.DESTROYED,
                    deploymentDao.byId(applied.id()).orElseThrow().status());
        }
    }
}
