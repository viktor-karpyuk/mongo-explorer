package com.kubrik.mex.k8s.portforward;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

/**
 * v2.8.1 Q2.8.1-C1 — Asks the kernel for a free ephemeral TCP port
 * on 127.0.0.1, then hands back an already-bound {@link ServerSocket}
 * so the caller can start accepting without a reserve/close race.
 *
 * <p>Different from the labs allocator: Labs only needs the port
 * <em>number</em> (docker then binds the port), so it closes the
 * reservation before returning. Port-forward sessions bind + listen
 * inside the app, so we keep the socket open and hand it to the
 * caller. There is no narrowing race and no retry needed.</p>
 */
public final class EphemeralPortAllocator {

    private EphemeralPortAllocator() {}

    /** Open a listener on a kernel-assigned ephemeral port on loopback. */
    public static ServerSocket reserveLoopback() throws IOException {
        ServerSocket ss = new ServerSocket();
        ss.setReuseAddress(true);
        ss.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        return ss;
    }
}
