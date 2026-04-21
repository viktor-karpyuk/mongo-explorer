package com.kubrik.mex.cluster.store;

import com.kubrik.mex.cluster.audit.OpsAuditRecord;
import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.store.Database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * v2.4 AUD-1..3 — DAO for {@code ops_audit}. Inserts return the record with
 * its assigned {@code id}; queries support the filter set from AUD-4..6
 * (connection, command name, outcome, time range). {@link #deleteForConnection}
 * participates in the cascade-delete transaction invoked when a connection row
 * is removed (see {@code ConnectionStore}).
 */
public final class OpsAuditDao {

    private final Database db;

    public OpsAuditDao(Database db) { this.db = db; }

    public OpsAuditRecord insert(OpsAuditRecord r) {
        String sql = """
                INSERT INTO ops_audit(
                    connection_id, db, coll, command_name, command_json_redacted,
                    preview_hash, outcome, server_message, role_used,
                    started_at, finished_at, latency_ms,
                    caller_host, caller_user, ui_source, paste, kill_switch
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        try (PreparedStatement ps = db.connection().prepareStatement(sql,
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, r.connectionId());
            setNullableString(ps, 2, r.db());
            setNullableString(ps, 3, r.coll());
            ps.setString(4, r.commandName());
            ps.setString(5, r.commandJsonRedacted());
            ps.setString(6, r.previewHash());
            ps.setString(7, r.outcome().name());
            setNullableString(ps, 8, r.serverMessage());
            setNullableString(ps, 9, r.roleUsed());
            ps.setLong(10, r.startedAt());
            setNullableLong(ps, 11, r.finishedAt());
            setNullableLong(ps, 12, r.latencyMs());
            setNullableString(ps, 13, r.callerHost());
            setNullableString(ps, 14, r.callerUser());
            ps.setString(15, r.uiSource());
            ps.setInt(16, r.paste() ? 1 : 0);
            ps.setInt(17, r.killSwitch() ? 1 : 0);
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                long id = keys.next() ? keys.getLong(1) : -1L;
                return r.withId(id);
            }
        } catch (SQLException e) {
            throw new RuntimeException("ops_audit insert failed", e);
        }
    }

    /**
     * Returns the most-recent rows for a connection, newest first. Unbounded
     * growth is controlled upstream by {@code AuditJanitor}.
     */
    public List<OpsAuditRecord> listForConnection(String connectionId, int limit) {
        String sql = "SELECT * FROM ops_audit WHERE connection_id = ? " +
                "ORDER BY started_at DESC, id DESC LIMIT ?";
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setString(1, connectionId);
            ps.setInt(2, limit);
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    public List<OpsAuditRecord> listSince(long sinceMs, int limit) {
        String sql = "SELECT * FROM ops_audit WHERE started_at >= ? " +
                "ORDER BY started_at DESC, id DESC LIMIT ?";
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setLong(1, sinceMs);
            ps.setInt(2, limit);
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    public OpsAuditRecord byId(long id) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM ops_audit WHERE id = ?")) {
            ps.setLong(1, id);
            List<OpsAuditRecord> rs = read(ps);
            return rs.isEmpty() ? null : rs.get(0);
        } catch (SQLException e) {
            return null;
        }
    }

    /** Cascade delete invoked in the connection-removal transaction. */
    public void deleteForConnection(Connection conn, String connectionId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM ops_audit WHERE connection_id = ?")) {
            ps.setString(1, connectionId);
            ps.executeUpdate();
        }
    }

    /* ------------------------------ internals ------------------------------ */

    private static List<OpsAuditRecord> read(PreparedStatement ps) throws SQLException {
        List<OpsAuditRecord> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(map(rs));
        }
        return out;
    }

    private static OpsAuditRecord map(ResultSet rs) throws SQLException {
        long finishedAt = rs.getLong("finished_at");
        boolean finishedNull = rs.wasNull();
        long latencyMs = rs.getLong("latency_ms");
        boolean latencyNull = rs.wasNull();
        return new OpsAuditRecord(
                rs.getLong("id"),
                rs.getString("connection_id"),
                rs.getString("db"),
                rs.getString("coll"),
                rs.getString("command_name"),
                rs.getString("command_json_redacted"),
                rs.getString("preview_hash"),
                Outcome.valueOf(rs.getString("outcome")),
                rs.getString("server_message"),
                rs.getString("role_used"),
                rs.getLong("started_at"),
                finishedNull ? null : finishedAt,
                latencyNull ? null : latencyMs,
                rs.getString("caller_host"),
                rs.getString("caller_user"),
                rs.getString("ui_source"),
                rs.getInt("paste") != 0,
                rs.getInt("kill_switch") != 0
        );
    }

    private static void setNullableString(PreparedStatement ps, int idx, String v) throws SQLException {
        if (v == null) ps.setNull(idx, Types.VARCHAR); else ps.setString(idx, v);
    }

    private static void setNullableLong(PreparedStatement ps, int idx, Long v) throws SQLException {
        if (v == null) ps.setNull(idx, Types.BIGINT); else ps.setLong(idx, v);
    }
}
