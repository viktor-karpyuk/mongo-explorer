package com.kubrik.mex.k8s.portforward;

import com.kubrik.mex.k8s.model.PortForwardTarget;
import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 * v2.8.1 Q2.8.1-C2 — CRUD over {@code portforward_audit}.
 *
 * <p>Two writes per session: {@link #insertOpen} at forward-up time,
 * {@link #markClosed} when the session tears down. Read paths stay
 * thin for now — the Audit pane aggregates these with the v2.4
 * {@code ops_audit} table in a later chunk.</p>
 *
 * <p>Writes go through the database's shared write-lock; the reads
 * are lock-free since SQLite already serialises readers against the
 * writer by way of the WAL journal.</p>
 */
public final class PortForwardAuditDao {

    private final Database database;

    public PortForwardAuditDao(Database database) {
        this.database = Objects.requireNonNull(database, "database");
    }

    /**
     * Write the open event. Returns the new row's id so the caller
     * can later update the {@code closed_at} / {@code reason_closed}
     * columns on teardown.
     */
    public long insertOpen(String connectionId, Long clusterId,
                            PortForwardTarget target, int localPort, long openedAt) throws SQLException {
        synchronized (database.writeLock()) {
            String sql = "INSERT INTO portforward_audit("
                    + " connection_id, k8s_cluster_id, namespace, "
                    + " target_kind, target_name, remote_port, local_port, opened_at) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = database.connection()
                    .prepareStatement(sql, java.sql.Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, connectionId);
                if (clusterId == null) ps.setNull(2, java.sql.Types.INTEGER);
                else ps.setLong(2, clusterId);
                ps.setString(3, target.namespace());
                ps.setString(4, target.kindLabel());
                ps.setString(5, target.name());
                ps.setInt(6, target.remotePort());
                ps.setInt(7, localPort);
                ps.setLong(8, openedAt);
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    return keys.next() ? keys.getLong(1) : -1L;
                }
            }
        }
    }

    /**
     * Flip {@code closed_at} + {@code reason_closed}. Idempotent: the
     * service calls this unconditionally on teardown so it runs once
     * per session even when multiple close paths (explicit close,
     * JVM exit hook, health-probe reconnect) race.
     */
    public void markClosed(long rowId, long closedAt, String reason) throws SQLException {
        synchronized (database.writeLock()) {
            try (PreparedStatement ps = database.connection().prepareStatement(
                    "UPDATE portforward_audit "
                    + "SET closed_at = ?, reason_closed = ? "
                    + "WHERE id = ? AND closed_at IS NULL")) {
                ps.setLong(1, closedAt);
                ps.setString(2, reason);
                ps.setLong(3, rowId);
                ps.executeUpdate();
            }
        }
    }

    /** Count open rows for a connection — used by tests + future UI. */
    public int countOpen(String connectionId) throws SQLException {
        try (PreparedStatement ps = database.connection().prepareStatement(
                "SELECT COUNT(*) FROM portforward_audit "
                + "WHERE connection_id = ? AND closed_at IS NULL")) {
            ps.setString(1, connectionId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }
}
