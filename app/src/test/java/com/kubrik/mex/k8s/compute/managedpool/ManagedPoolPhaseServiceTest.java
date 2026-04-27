package com.kubrik.mex.k8s.compute.managedpool;

import com.kubrik.mex.k8s.apply.ProvisioningRecordDao;
import com.kubrik.mex.k8s.cluster.KubeClusterDao;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.4 Q2.8.4-D — Cloud phase end-to-end through the EKS stub.
 * Asserts the phase service drives create → describe → Ready and
 * stamps the cloud.* audit rows along the way.
 */
class ManagedPoolPhaseServiceTest {

    @TempDir Path dataDir;
    private Database db;
    private CloudCredentialDao credDao;
    private ManagedPoolOperationDao opDao;
    private ManagedPoolPhaseService phase;
    private long credentialId;
    private ProvisioningRecordDao recordDao;
    private long clusterId;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        credDao = new CloudCredentialDao(db);
        opDao = new ManagedPoolOperationDao(db);
        credentialId = credDao.insert("test-eks", CloudProvider.AWS,
                CloudCredential.AuthMode.STATIC, "keychain://x",
                Optional.of("123456789012"), Optional.empty(),
                Optional.empty(), Optional.of("us-east-1"));
        phase = new ManagedPoolPhaseService(
                ManagedPoolAdapterRegistry.defaultRegistry(),
                credDao, opDao);
        recordDao = new ProvisioningRecordDao(db);
        clusterId = new KubeClusterDao(db).insert(
                "t", "/k", "ctx", Optional.empty(), Optional.empty());
    }

    /** Insert a placeholder provisioning_records row so the audit
     *  appender's FK can land. Returns the row id. */
    private long newProvisioningRow() throws Exception {
        return recordDao.insertApplying(clusterId, "mongo", "rs",
                "MCO", "0.0", "7.0", "RS3", "DEV_TEST",
                "yaml: ok", "sha", false);
    }

    @AfterEach
    void tearDown() throws Exception { if (db != null) db.close(); }

    @Test
    void create_phase_succeeds_and_audits_each_call() throws Exception {
        long pid = newProvisioningRow();
        ManagedPoolSpec spec = ManagedPoolSpec.sensibleEksDefaults(
                credentialId, "us-east-1", "rs0");
        ManagedPoolPhaseService.Result r = phase.createAndAwaitReady(pid, spec);
        assertTrue(r.ok(), r.errorMessage().orElse(""));
        assertTrue(r.cloudCallId().orElse("").startsWith("eks-stub-"));
    }

    @Test
    void audit_rows_carry_provider_action_account_and_call_id() throws Exception {
        long pid = newProvisioningRow();
        ManagedPoolSpec spec = ManagedPoolSpec.sensibleEksDefaults(
                credentialId, "us-east-1", "rs1");
        phase.createAndAwaitReady(pid, spec);

        List<ManagedPoolOperationDao.Row> rows = opDao.listForProvision(pid);
        assertFalse(rows.isEmpty());
        var first = rows.get(0);
        assertEquals(CloudProvider.AWS, first.provider());
        assertEquals(ManagedPoolOperationDao.Action.POOL_CREATE, first.action());
        assertEquals(Optional.of("123456789012"), first.accountId());
        assertEquals(Optional.of("us-east-1"), first.region());
        assertTrue(first.cloudCallId().isPresent());
        assertEquals(ManagedPoolOperationDao.Status.ACCEPTED, first.status());

        // At least one POOL_DESCRIBE row should follow.
        assertTrue(rows.stream().anyMatch(row -> row.action()
                == ManagedPoolOperationDao.Action.POOL_DESCRIBE));
    }

    @Test
    void delete_phase_audits_pool_delete_with_cloud_call_id() throws Exception {
        long pid = newProvisioningRow();
        ManagedPoolSpec spec = ManagedPoolSpec.sensibleEksDefaults(
                credentialId, "us-east-1", "rs2");
        phase.createAndAwaitReady(pid, spec);
        ManagedPoolPhaseService.Result del = phase.delete(pid, spec);
        assertTrue(del.ok(), del.errorMessage().orElse(""));

        List<ManagedPoolOperationDao.Row> rows = opDao.listForProvision(pid);
        assertTrue(rows.stream().anyMatch(row -> row.action()
                == ManagedPoolOperationDao.Action.POOL_DELETE));
    }

    @Test
    void unknown_provider_fails_the_phase_with_actionable_message() throws Exception {
        long azureCred = credDao.insert("azure", CloudProvider.AZURE,
                CloudCredential.AuthMode.MANAGED_IDENTITY, "keychain://az",
                Optional.empty(), Optional.empty(),
                Optional.of("sub-1"), Optional.of("eastus"));
        // Build a registry that intentionally has no Azure adapter.
        ManagedPoolAdapterRegistry empty = new ManagedPoolAdapterRegistry();
        ManagedPoolPhaseService bare = new ManagedPoolPhaseService(empty, credDao, opDao);
        ManagedPoolSpec spec = new ManagedPoolSpec(CloudProvider.AZURE, azureCred,
                "eastus", "mex-rs", "Standard_D2s_v5",
                ManagedPoolSpec.CapacityType.ON_DEMAND, 3, 3, 5, "amd64",
                List.of(), List.of());
        ManagedPoolPhaseService.Result r = bare.createAndAwaitReady(1L, spec);
        assertFalse(r.ok());
        assertTrue(r.errorMessage().orElse("").contains("AZURE"));
    }
}
