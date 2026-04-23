package com.kubrik.mex.k8s.portforward;

import com.kubrik.mex.k8s.model.PortForwardTarget;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class PortForwardTargetTest {

    @Test
    void for_service_factory_populates_service_and_kind() {
        PortForwardTarget t = PortForwardTarget.forService("mongo", "mongo-svc", 27017);
        assertEquals("mongo", t.namespace());
        assertEquals("SERVICE", t.kindLabel());
        assertEquals("mongo-svc", t.name());
        assertTrue(t.pod().isEmpty());
    }

    @Test
    void for_pod_factory_populates_pod_and_kind() {
        PortForwardTarget t = PortForwardTarget.forPod("db", "db-0", 27017);
        assertEquals("POD", t.kindLabel());
        assertEquals("db-0", t.name());
        assertTrue(t.service().isEmpty());
    }

    @Test
    void blank_namespace_rejected() {
        assertThrows(IllegalArgumentException.class, () ->
                new PortForwardTarget("", Optional.empty(), Optional.of("x"), 27017));
    }

    @Test
    void port_zero_and_above_65535_rejected() {
        assertThrows(IllegalArgumentException.class, () ->
                PortForwardTarget.forService("mongo", "svc", 0));
        assertThrows(IllegalArgumentException.class, () ->
                PortForwardTarget.forService("mongo", "svc", 70_000));
    }

    @Test
    void both_pod_and_service_rejected_xor() {
        assertThrows(IllegalArgumentException.class, () ->
                new PortForwardTarget("mongo",
                        Optional.of("p"), Optional.of("s"), 27017));
        assertThrows(IllegalArgumentException.class, () ->
                new PortForwardTarget("mongo",
                        Optional.empty(), Optional.empty(), 27017));
    }
}
