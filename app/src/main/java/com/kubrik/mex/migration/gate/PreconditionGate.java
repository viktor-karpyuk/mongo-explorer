package com.kubrik.mex.migration.gate;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.model.ConnectionState;
import com.kubrik.mex.model.MongoConnection;
import com.kubrik.mex.store.ConnectionStore;

/** Capability checks that must pass before a job is allowed to start.
 *  <p>
 *  MVP scope: SSH not in use (SEC-3 deferred), server versions ≥ 5.0, and connection
 *  reachability. Kept deliberately simple so it can be unit-tested against a mocked
 *  {@link ConnectionStore} / {@link ConnectionManager}.
 *  <p>
 *  See docs/mvp-technical-spec.md §13.1 for the full rationale. */
public final class PreconditionGate {

    private static final int MIN_MAJOR_VERSION = 5;

    private final ConnectionStore store;
    private final ConnectionManager manager;

    public PreconditionGate(ConnectionStore store, ConnectionManager manager) {
        this.store = store;
        this.manager = manager;
    }

    public void check(MigrationSpec spec) {
        checkConnection("Source", spec.source().connectionId());
        checkConnection("Target", spec.target().connectionId());
    }

    private void checkConnection(String role, String connId) {
        if (connId == null || connId.isBlank()) {
            throw new PreconditionException(role + " connection is not set.");
        }
        MongoConnection conn = store.get(connId);
        if (conn == null) {
            throw new PreconditionException(role + " connection `" + connId + "` is unknown.");
        }
        if (conn.sshEnabled()) {
            // E-12 — message kept close to the functional spec so UX can surface it verbatim.
            throw new PreconditionException(
                    "This connection uses SSH tunnelling, which is not available in this version. "
                  + "Disable SSH on the connection or wait for SSH tunnel support (SEC-3).");
        }
        ConnectionState state = manager.state(connId);
        if (state.status() == ConnectionState.Status.ERROR) {
            throw new PreconditionException(role + " connection `" + conn.name()
                    + "` is in error state: " + state.lastError());
        }
        MongoService svc = manager.service(connId);
        if (svc != null) {
            requireMinVersion(role, conn.name(), svc.serverVersion());
        }
    }

    static void requireMinVersion(String role, String name, String version) {
        if (version == null || version.isBlank()) return;
        int dot = version.indexOf('.');
        String majorStr = dot > 0 ? version.substring(0, dot) : version;
        try {
            int major = Integer.parseInt(majorStr);
            if (major < MIN_MAJOR_VERSION) {
                throw new PreconditionException(role + " connection `" + name
                        + "` runs MongoDB " + version + "; migration requires 5.0 or newer.");
            }
        } catch (NumberFormatException ignored) {
            // Unparsable version string — don't block, just let preflight surface it later.
        }
    }
}
