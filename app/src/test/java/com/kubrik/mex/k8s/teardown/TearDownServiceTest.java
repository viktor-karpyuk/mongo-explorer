package com.kubrik.mex.k8s.teardown;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.k8s.apply.ApplyOrchestrator;
import com.kubrik.mex.k8s.apply.ProvisioningRecordDao;
import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.cluster.KubeClusterDao;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.model.ProvisioningRecord;
import com.kubrik.mex.k8s.rollout.ResourceCatalogue;
import com.kubrik.mex.store.Database;
import io.kubernetes.client.openapi.ApiClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class TearDownServiceTest {

    @TempDir Path dataDir;

    private Database db;
    private ProvisioningRecordDao recordDao;
    private long clusterId;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        recordDao = new ProvisioningRecordDao(db);
        clusterId = new KubeClusterDao(db).insert(
                "t", "/k", "ctx", Optional.empty(), Optional.empty());
    }

    @AfterEach
    void tearDown() throws Exception { if (db != null) db.close(); }

    @Test
    void protected_row_refuses_teardown_until_protection_cleared() throws Exception {
        long rowId = insertRow("prod", true);
        Recorder opener = new Recorder();
        TearDownService svc = new TearDownService(
                recordDao, new Stub(), opener, new EventBus());

        assertThrows(IllegalStateException.class, () ->
                svc.tearDown(ref(), rowId, CascadePlan.prodDefaults()));

        svc.clearDeletionProtection(rowId);
        TearDownService.TearDownResult r =
                svc.tearDown(ref(), rowId, CascadePlan.prodDefaults());
        assertNotNull(r);
    }

    @Test
    void unprotected_row_tears_down_on_first_call() throws Exception {
        long rowId = insertRow("dev", false);
        Recorder opener = new Recorder();
        TearDownService svc = new TearDownService(
                recordDao, new Stub(), opener, new EventBus());

        TearDownService.TearDownResult r = svc.tearDown(
                ref(), rowId, CascadePlan.devDefaults());
        assertEquals(3, r.deletedCount(), "dev cascade deletes CR + Secrets + PVC");
        assertEquals("clean", r.summary());
        assertEquals(ProvisioningRecord.Status.DELETED,
                recordDao.findById(rowId).orElseThrow().status());
    }

    @Test
    void prod_cascade_keeps_pvcs_by_default() throws Exception {
        long rowId = insertRow("prod", false);
        Recorder opener = new Recorder();
        TearDownService svc = new TearDownService(
                recordDao, new Stub(), opener, new EventBus());

        TearDownService.TearDownResult r = svc.tearDown(
                ref(), rowId, CascadePlan.prodDefaults());
        assertEquals(1, r.deletedCount(),
                "prod default is CR only — Secrets and PVCs preserved");
        assertFalse(opener.deletes.stream()
                .anyMatch(ref -> ref.kind().equals("PersistentVolumeClaim")));
    }

    @Test
    void opener_failures_are_recorded_but_dont_abort() throws Exception {
        long rowId = insertRow("dev", false);
        Flaky opener = new Flaky();
        TearDownService svc = new TearDownService(
                recordDao, new Stub(), opener, new EventBus());

        TearDownService.TearDownResult r = svc.tearDown(
                ref(), rowId, CascadePlan.devDefaults());
        assertEquals("partial", r.summary());
        assertFalse(r.failures().isEmpty());
        // Row still flipped to DELETED so the UI doesn't block on a stuck teardown.
        assertEquals(ProvisioningRecord.Status.DELETED,
                recordDao.findById(rowId).orElseThrow().status());
    }

    @Test
    void double_teardown_is_safe_noop() throws Exception {
        long rowId = insertRow("dev", false);
        TearDownService svc = new TearDownService(
                recordDao, new Stub(), new Recorder(), new EventBus());
        svc.tearDown(ref(), rowId, CascadePlan.devDefaults());
        TearDownService.TearDownResult r = svc.tearDown(
                ref(), rowId, CascadePlan.devDefaults());
        assertEquals("already deleted", r.summary());
        assertEquals(0, r.deletedCount());
    }

    private K8sClusterRef ref() {
        return new K8sClusterRef(clusterId, "t", "/k", "ctx",
                Optional.empty(), Optional.empty(), 0L, Optional.empty());
    }

    private long insertRow(String name, boolean protection) throws Exception {
        return recordDao.insertApplying(clusterId, "mongo", name,
                "MCO", "x", "7.0", "RS3", "DEV_TEST",
                "cr: yaml", "sha-hex", protection);
    }

    static final class Stub extends KubeClientFactory {
        @Override public ApiClient get(K8sClusterRef ref) { return null; }
    }

    static final class Recorder implements ApplyOrchestrator.ApplyOpener {
        final List<ResourceCatalogue.Ref> deletes = new ArrayList<>();
        @Override public void apply(ApiClient c, ResourceCatalogue.Ref r, String y) {}
        @Override public void delete(ApiClient c, ResourceCatalogue.Ref r) { deletes.add(r); }
    }

    static final class Flaky implements ApplyOrchestrator.ApplyOpener {
        int call = 0;
        @Override public void apply(ApiClient c, ResourceCatalogue.Ref r, String y) {}
        @Override public void delete(ApiClient c, ResourceCatalogue.Ref r) throws Exception {
            call++;
            if (call == 2) throw new RuntimeException("synthetic delete failure");
        }
    }
}
