package com.kubrik.mex.k8s.provision;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TopologyPickerTest {

    @Test
    void dev_test_limits_to_standalone_and_rs3() {
        Set<Topology> mco = TopologyPicker.availableTopologies(Profile.DEV_TEST, OperatorId.MCO);
        assertEquals(Set.of(Topology.STANDALONE, Topology.RS3), mco);
        Set<Topology> psmdb = TopologyPicker.availableTopologies(Profile.DEV_TEST, OperatorId.PSMDB);
        assertEquals(Set.of(Topology.STANDALONE, Topology.RS3), psmdb);
    }

    @Test
    void prod_mco_offers_rs3_rs5_only_no_sharded() {
        Set<Topology> avail = TopologyPicker.availableTopologies(Profile.PROD, OperatorId.MCO);
        assertEquals(Set.of(Topology.RS3, Topology.RS5), avail);
        assertFalse(avail.contains(Topology.SHARDED));
    }

    @Test
    void prod_psmdb_offers_sharded() {
        Set<Topology> avail = TopologyPicker.availableTopologies(Profile.PROD, OperatorId.PSMDB);
        assertTrue(avail.contains(Topology.SHARDED));
    }

    @Test
    void default_for_switches_to_rs3_when_current_invalid() {
        assertEquals(Topology.RS3,
                TopologyPicker.defaultFor(Profile.PROD, OperatorId.MCO, Topology.SHARDED));
        assertEquals(Topology.RS3,
                TopologyPicker.defaultFor(Profile.PROD, OperatorId.MCO, Topology.STANDALONE));
    }

    @Test
    void default_for_preserves_valid_current() {
        assertEquals(Topology.RS5,
                TopologyPicker.defaultFor(Profile.PROD, OperatorId.PSMDB, Topology.RS5));
    }

    @Test
    void allows_sharded_only_for_prod_psmdb() {
        assertTrue(TopologyPicker.allowsSharded(Profile.PROD, OperatorId.PSMDB));
        assertFalse(TopologyPicker.allowsSharded(Profile.DEV_TEST, OperatorId.PSMDB));
        assertFalse(TopologyPicker.allowsSharded(Profile.PROD, OperatorId.MCO));
    }
}
