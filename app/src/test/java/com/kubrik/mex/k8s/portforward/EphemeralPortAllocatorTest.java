package com.kubrik.mex.k8s.portforward;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class EphemeralPortAllocatorTest {

    @Test
    void reserve_opens_on_loopback_with_nonzero_port() throws IOException {
        try (ServerSocket ss = EphemeralPortAllocator.reserveLoopback()) {
            assertTrue(ss.getLocalPort() > 0);
            assertEquals(InetAddress.getLoopbackAddress(), ss.getInetAddress());
            assertFalse(ss.isClosed());
        }
    }

    @Test
    void reserves_distinct_ports_on_successive_calls() throws IOException {
        Set<Integer> ports = new HashSet<>();
        ServerSocket a = EphemeralPortAllocator.reserveLoopback();
        ServerSocket b = EphemeralPortAllocator.reserveLoopback();
        try {
            ports.add(a.getLocalPort());
            ports.add(b.getLocalPort());
            assertEquals(2, ports.size(), "allocator must hand out distinct ports");
        } finally {
            a.close(); b.close();
        }
    }
}
