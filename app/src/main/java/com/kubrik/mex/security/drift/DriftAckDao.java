package com.kubrik.mex.security.drift;

import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * v2.6 Q2.6-D3 — DAO for {@code sec_drift_acks}. Writes flow through the
 * {@link Database#writeLock} per the SQLite write-serialisation contract.
 * MUTEs are scoped by {@code connection_id} + {@code path}; ACKs are
 * additionally keyed on {@code baseline_id} (foreign key with cascade).
 */
public final class DriftAckDao {

    private final Database db;

    public DriftAckDao(Database db) { this.db = db; }

    public DriftAck insert(DriftAck row) {
        String sql = """
                INSERT INTO sec_drift_acks(connection_id, baseline_id, path,
                    acked_at, acked_by, mode, note)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql,
                    Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, row.connectionId());
                ps.setLong(2, row.baselineId());
                ps.setString(3, row.path());
                ps.setLong(4, row.ackedAt());
                ps.setString(5, row.ackedBy());
                ps.setString(6, row.mode().name());
                if (row.note() == null || row.note().isEmpty())
                    ps.setNull(7, java.sql.Types.VARCHAR);
                else ps.setString(7, row.note());
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    long id = keys.next() ? keys.getLong(1) : -1L;
                    return new DriftAck(id, row.connectionId(), row.baselineId(),
                            row.path(), row.ackedAt(), row.ackedBy(),
                            row.mode(), row.note());
                }
            } catch (SQLException e) {
                throw new RuntimeException("sec_drift_acks insert failed", e);
            }
        }
    }

    /** All acks + mutes for {@code connectionId}. The drift pane filters
     *  findings client-side against this list — cheaper than a SQL join
     *  and keeps the filter logic in one place ({@link DriftAck#hideAcked}). */
    public List<DriftAck> listForConnection(String connectionId) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM sec_drift_acks WHERE connection_id = ? " +
                        "ORDER BY acked_at DESC")) {
            ps.setString(1, connectionId);
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** Remove a specific ack or mute (the pane's "Un-ack" / "Un-mute"
     *  button). Returns {@code true} if a row was deleted. */
    public boolean delete(long id) {
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(
                    "DELETE FROM sec_drift_acks WHERE id = ?")) {
                ps.setLong(1, id);
                return ps.executeUpdate() > 0;
            } catch (SQLException e) {
                return false;
            }
        }
    }

    /* ============================= internals ============================= */

    private List<DriftAck> read(PreparedStatement ps) throws SQLException {
        List<DriftAck> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(map(rs));
        }
        return out;
    }

    private static DriftAck map(ResultSet rs) throws SQLException {
        return new DriftAck(
                rs.getLong("id"),
                rs.getString("connection_id"),
                rs.getLong("baseline_id"),
                rs.getString("path"),
                rs.getLong("acked_at"),
                rs.getString("acked_by"),
                DriftAck.Mode.valueOf(rs.getString("mode")),
                rs.getString("note"));
    }
}
