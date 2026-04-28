package com.kubrik.mex.labs.store;

import com.kubrik.mex.labs.model.LabDeployment;
import com.kubrik.mex.labs.model.LabStatus;
import com.kubrik.mex.labs.model.PortMap;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class LabDeploymentDaoTest {

    @TempDir Path dataDir;

    private Database db;
    private LabDeploymentDao dao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        dao = new LabDeploymentDao(db);
    }

    @AfterEach
    void tearDown() throws Exception { if (db != null) db.close(); }

    @Test
    void round_trips_lab_with_port_map_and_status() {
        Map<String, Integer> ports = new LinkedHashMap<>();
        ports.put("mongos", 27100);
        ports.put("cfg1", 27101);
        LabDeployment in = new LabDeployment(-1, "sharded-1", "2.8.4-alpha",
                "My shard lab", "mex-lab-sharded-1-abc12345",
                "/tmp/mex-lab/compose.yml",
                new PortMap(ports), LabStatus.CREATING,
                /*keepDataOnStop=*/false, /*authEnabled=*/false,
                1_700_000_000_000L, Optional.empty(), Optional.empty(),
                Optional.empty(), "mongo:latest", Optional.empty());
        LabDeployment saved = dao.insert(in);
        assertTrue(saved.id() > 0);

        LabDeployment back = dao.byComposeProject(
                "mex-lab-sharded-1-abc12345").orElseThrow();
        assertEquals(LabStatus.CREATING, back.status());
        assertEquals(27100, back.portMap().portFor("mongos"));
        assertEquals(27101, back.portMap().portFor("cfg1"));
        assertFalse(back.keepDataOnStop());
    }

    @Test
    void updateStatus_writes_timestamp_column() {
        LabDeployment saved = insertSample("mex-lab-standalone-1");
        assertTrue(dao.updateStatus(saved.id(), LabStatus.RUNNING,
                1_700_000_001_000L, "last_started_at"));

        LabDeployment back = dao.byId(saved.id()).orElseThrow();
        assertEquals(LabStatus.RUNNING, back.status());
        assertEquals(Optional.of(1_700_000_001_000L), back.lastStartedAt());
    }

    @Test
    void listLive_excludes_destroyed() {
        insertSample("mex-lab-a-1");
        LabDeployment b = insertSample("mex-lab-b-2");
        dao.updateStatus(b.id(), LabStatus.DESTROYED, 1_700_000_002_000L,
                "destroyed_at");
        var live = dao.listLive();
        assertEquals(1, live.size());
        assertEquals("mex-lab-a-1", live.get(0).composeProject());
    }

    @Test
    void setConnectionId_persists() {
        LabDeployment saved = insertSample("mex-lab-c-3");
        String cx = java.util.UUID.randomUUID().toString();
        assertTrue(dao.setConnectionId(saved.id(), cx));
        assertEquals(Optional.of(cx), dao.byId(saved.id())
                .orElseThrow().connectionId());
    }

    private LabDeployment insertSample(String project) {
        return dao.insert(new LabDeployment(-1, "standalone", "2.8.4-alpha",
                "Lab", project, "/tmp/compose.yml",
                new PortMap(Map.of("mongo", 27100)),
                LabStatus.CREATING, false, false,
                1_700_000_000_000L, Optional.empty(), Optional.empty(),
                Optional.empty(), "mongo:latest", Optional.empty()));
    }
}
