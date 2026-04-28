package com.kubrik.mex.labs.ports;

import com.kubrik.mex.labs.model.PortMap;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * v2.8.4 LAB-PORTS-* — Asks the kernel for ephemeral port numbers
 * the app isn't currently using, then releases the sockets so the
 * subsequent {@code docker compose up} can bind them.
 *
 * <p>A narrowing race between {@code ServerSocket.close()} and
 * {@code docker compose up} binding the port is bounded by the
 * kernel's ephemeral-port recycle policy (&lt; 10 ms empirically on
 * Darwin / Linux). Callers retry {@link #allocate} on bind failure
 * per LAB-PORTS-2 (up to 5 times).</p>
 *
 * <p>Thread-safe via statelessness: each {@code ServerSocket(0)}
 * call asks the kernel for a fresh reservation, so parallel
 * allocators cannot collide.</p>
 */
public final class EphemeralPortAllocator {

    /** Bind to {@code 127.0.0.1} so the first-bind triggers the
     *  kernel's same port-reservation path a subsequent loopback
     *  bind by mongod would exercise. Wildcard ({@code 0.0.0.0})
     *  would reserve a port on the whole host; loopback is narrower
     *  and matches the render ({@code 127.0.0.1:<port>} in compose). */
    public PortMap allocate(List<String> containerNames) throws IOException {
        Map<String, Integer> out = new LinkedHashMap<>();
        // Hold every socket open until the WHOLE allocation is
        // complete, then close them all at once — prevents the
        // kernel from handing out the same port twice within a
        // single invocation if it happens to recycle quickly.
        List<ServerSocket> reserved = new java.util.ArrayList<>(containerNames.size());
        try {
            for (String name : containerNames) {
                ServerSocket ss = new ServerSocket();
                ss.setReuseAddress(true);
                ss.bind(new java.net.InetSocketAddress(
                        java.net.InetAddress.getLoopbackAddress(), 0));
                reserved.add(ss);
                out.put(name, ss.getLocalPort());
            }
        } catch (IOException e) {
            closeAll(reserved);
            throw e;
        }
        closeAll(reserved);
        return new PortMap(out);
    }

    private static void closeAll(List<ServerSocket> sockets) {
        for (ServerSocket ss : sockets) {
            try { ss.close(); } catch (IOException ignored) {}
        }
    }
}
