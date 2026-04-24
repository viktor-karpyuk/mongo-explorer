package com.kubrik.mex.k8s.compute.managedpool;

import com.kubrik.mex.k8s.compute.ComputeStrategy;
import com.kubrik.mex.k8s.compute.ComputeStrategyJson;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.4 Q2.8.4-A/B — Codec round-trip + DAO + stub adapter
 * invariants. The real cloud adapter (AWS SDK-backed) ships once
 * the OS-keychain integration for {@link CloudCredential#keychainRef}
 * is security-reviewed; until then the EKS stub proves the wiring
 * end to end.
 */
class ManagedPoolJsonAndAdapterTest {

    @TempDir Path dataDir;
    private Database db;
    private CloudCredentialDao dao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        dao = new CloudCredentialDao(db);
    }

    @AfterEach
    void tearDown() throws Exception { if (db != null) db.close(); }

    @Test
    void managed_pool_round_trips_full_spec() {
        ComputeStrategy.ManagedPool original = new ComputeStrategy.ManagedPool(
                ManagedPoolSpec.sensibleEksDefaults(7L, "us-east-1", "prod-rs"));
        String json = ComputeStrategyJson.toJson(original);
        assertTrue(json.contains("managed_pool"));
        assertTrue(json.contains("us-east-1"));
        assertTrue(json.contains("m6i.large"));
        assertTrue(json.contains("ON_DEMAND"));

        ComputeStrategy.ManagedPool back = (ComputeStrategy.ManagedPool)
                ComputeStrategyJson.fromJson(json);
        assertTrue(back.spec().isPresent());
        ManagedPoolSpec sp = back.spec().get();
        assertEquals(CloudProvider.AWS, sp.provider());
        assertEquals(7L, sp.credentialId());
        assertEquals("us-east-1", sp.region());
        assertEquals("mex-prod-rs", sp.poolName());
        assertEquals("m6i.large", sp.instanceType());
        assertEquals(ManagedPoolSpec.CapacityType.ON_DEMAND, sp.capacityType());
        assertEquals(3, sp.minNodes());
        assertEquals(5, sp.maxNodes());
    }

    @Test
    void bare_managed_pool_without_spec_round_trips_as_type_tag() {
        String json = ComputeStrategyJson.toJson(new ComputeStrategy.ManagedPool());
        assertNotNull(json);
        ComputeStrategy.ManagedPool back = (ComputeStrategy.ManagedPool)
                ComputeStrategyJson.fromJson(json);
        assertTrue(back.spec().isEmpty());
    }

    @Test
    void invalid_node_count_rejected_at_construction() {
        assertThrows(IllegalArgumentException.class,
                () -> new ManagedPoolSpec(CloudProvider.AWS, 1L, "us-east-1",
                        "x", "m6i.large", ManagedPoolSpec.CapacityType.ON_DEMAND,
                        5, 3, 4, "amd64", java.util.List.of(), java.util.List.of()),
                "desired < min must throw");
    }

    @Test
    void cloud_credentials_dao_round_trips_a_row() throws Exception {
        long id = dao.insert("prod-eks", CloudProvider.AWS,
                CloudCredential.AuthMode.IRSA, "keychain://mex/aws/prod-eks",
                Optional.of("123456789012"), Optional.empty(), Optional.empty(),
                Optional.of("us-east-1"));
        assertTrue(id > 0);

        CloudCredential back = dao.findById(id).orElseThrow();
        assertEquals("prod-eks", back.displayName());
        assertEquals(CloudProvider.AWS, back.provider());
        assertEquals(CloudCredential.AuthMode.IRSA, back.authMode());
        assertEquals("keychain://mex/aws/prod-eks", back.keychainRef());
        assertEquals(Optional.of("123456789012"), back.awsAccountId());

        dao.recordProbe(id, CloudCredential.ProbeStatus.OK);
        CloudCredential probed = dao.findById(id).orElseThrow();
        assertEquals(Optional.of(CloudCredential.ProbeStatus.OK), probed.probeStatus());
        assertTrue(probed.lastProbedAt().isPresent());
    }

    @Test
    void eks_stub_records_create_then_progresses_to_ready_on_describe() {
        EksAdapterStub adapter = new EksAdapterStub();
        ManagedPoolSpec sp = ManagedPoolSpec.sensibleEksDefaults(1L, "us-east-1", "dev-rs");
        CloudCredential dummy = new CloudCredential(1L, "dummy", CloudProvider.AWS,
                CloudCredential.AuthMode.STATIC, "keychain://x",
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                0L, Optional.empty(), Optional.empty());

        var result = adapter.createPool(dummy, sp);
        assertEquals(ManagedPoolAdapter.PoolOperationResult.Status.ACCEPTED, result.status());
        assertTrue(result.cloudCallId().orElse("").startsWith("eks-stub-"));

        // First describe — still creating.
        var first = adapter.describe(dummy, sp.region(), sp.poolName()).orElseThrow();
        assertEquals(ManagedPoolAdapter.PoolPhase.READY, first.phase(),
                "stub flips to READY on first describe so tests can assert a single tick");

        adapter.deletePool(dummy, sp.region(), sp.poolName());
        assertTrue(adapter.describe(dummy, sp.region(), sp.poolName()).isEmpty());
    }

    @Test
    void registry_looks_up_registered_adapter() {
        ManagedPoolAdapterRegistry reg = ManagedPoolAdapterRegistry.defaultRegistry();
        assertTrue(reg.lookup(CloudProvider.AWS).isPresent());
        assertTrue(reg.lookup(CloudProvider.GCP).isEmpty());
        assertTrue(reg.lookup(CloudProvider.AZURE).isEmpty());
    }
}
