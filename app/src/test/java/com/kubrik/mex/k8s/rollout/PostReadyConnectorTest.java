package com.kubrik.mex.k8s.rollout;

import com.kubrik.mex.k8s.provision.OperatorId;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.k8s.provision.Topology;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PostReadyConnectorTest {

    @Test
    void mco_service_name_uses_svc_suffix() {
        ProvisionModel m = ProvisionModel.defaults(1L, "mongo", "my-rs");
        assertEquals("my-rs-svc", PostReadyConnector.serviceNameFor(m));
    }

    @Test
    void psmdb_non_sharded_uses_rs0_suffix() {
        ProvisionModel m = ProvisionModel.defaults(1L, "mongo", "my-rs")
                .withOperator(OperatorId.PSMDB);
        assertEquals("my-rs-rs0", PostReadyConnector.serviceNameFor(m));
    }

    @Test
    void psmdb_sharded_uses_mongos_suffix() {
        ProvisionModel m = ProvisionModel.defaults(1L, "mongo", "shards")
                .withOperator(OperatorId.PSMDB)
                .withTopology(Topology.SHARDED);
        assertEquals("shards-mongos", PostReadyConnector.serviceNameFor(m));
    }

    @Test
    void label_combines_operator_and_coords() {
        ProvisionModel mco = ProvisionModel.defaults(1L, "mongo", "prod");
        assertEquals("mco:mongo/prod", PostReadyConnector.labelFor(mco));
        ProvisionModel psmdb = mco.withOperator(OperatorId.PSMDB);
        assertEquals("psmdb:mongo/prod", PostReadyConnector.labelFor(psmdb));
    }

    @Test
    void connect_outcome_factories() {
        com.kubrik.mex.k8s.model.PortForwardSession session =
                new com.kubrik.mex.k8s.model.PortForwardSession(
                        1L, "c-1",
                        com.kubrik.mex.k8s.model.PortForwardTarget.forService("ns", "svc", 27017),
                        31000, 0L, 42L);
        PostReadyConnector.ConnectOutcome ok =
                PostReadyConnector.ConnectOutcome.ok("c-1", session);
        assertTrue(ok.ok());
        assertEquals("c-1", ok.connectionId().orElseThrow());
        assertEquals(session, ok.session().orElseThrow());
        assertTrue(ok.failureReason().isEmpty());

        PostReadyConnector.ConnectOutcome fail =
                PostReadyConnector.ConnectOutcome.failed("boom");
        assertFalse(fail.ok());
        assertEquals("boom", fail.failureReason().orElseThrow());
        assertTrue(fail.connectionId().isEmpty());
        assertTrue(fail.session().isEmpty());
    }
}
