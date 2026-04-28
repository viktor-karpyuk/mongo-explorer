package com.kubrik.mex.k8s.preflight.checks;

import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.k8s.provision.Topology;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class QuotaCheckTest {

    @Test
    void rs3_storage_estimate_multiplies_size_by_members() {
        ProvisionModel m = ProvisionModel.defaults(1L, "mongo", "rs3");
        assertEquals(30L, QuotaCheck.estimateStorageGib(m),
                "dev defaults: 10Gi × 3 members × 1 shard = 30Gi");
    }

    @Test
    void sharded_estimate_includes_config_servers() {
        ProvisionModel m = ProvisionModel.defaults(1L, "mongo", "shard")
                .withTopology(Topology.SHARDED);
        // 10Gi × 3 × 3 + 2Gi × 3 = 90 + 6 = 96
        assertEquals(96L, QuotaCheck.estimateStorageGib(m));
    }

    @Test
    void gib_suffix_parses() {
        assertEquals(10L, QuotaCheck.toGib("10Gi"));
        assertEquals(1024L, QuotaCheck.toGib("1Ti"));
    }

    @Test
    void mi_rounds_to_zero_gib() {
        assertEquals(0L, QuotaCheck.toGib("100Mi"));
    }

    @Test
    void unparseable_returns_zero() {
        assertEquals(0L, QuotaCheck.toGib("garbage"));
        assertEquals(0L, QuotaCheck.toGib(null));
    }
}
