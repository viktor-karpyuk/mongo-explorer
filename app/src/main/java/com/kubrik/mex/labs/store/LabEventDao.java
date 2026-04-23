package com.kubrik.mex.labs.store;

import com.kubrik.mex.labs.model.LabEvent;
import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * v2.8.4 — DAO for {@code lab_events}. Append-only.
 */
public final class LabEventDao {

    private final Database db;

    public LabEventDao(Database db) { this.db = db; }

    public long insert(long labId, LabEvent.Kind kind, long at, String message) {
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(
                    "INSERT INTO lab_events(lab_id, at, kind, message) " +
                    "VALUES (?, ?, ?, ?)",
                    java.sql.Statement.RETURN_GENERATED_KEYS)) {
                ps.setLong(1, labId);
                ps.setLong(2, at);
                ps.setString(3, kind.name());
                if (message == null) ps.setNull(4, java.sql.Types.VARCHAR);
                else ps.setString(4, message);
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    return keys.next() ? keys.getLong(1) : -1L;
                }
            } catch (SQLException e) {
                throw new RuntimeException("lab_events insert failed", e);
            }
        }
    }

    public List<LabEvent> listForLab(long labId, int limit) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM lab_events WHERE lab_id = ? " +
                "ORDER BY at DESC, id DESC LIMIT ?")) {
            ps.setLong(1, labId);
            ps.setInt(2, limit);
            List<LabEvent> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(map(rs));
            }
            return out;
        } catch (SQLException e) {
            return List.of();
        }
    }

    private static LabEvent map(ResultSet rs) throws SQLException {
        return new LabEvent(
                rs.getLong("id"),
                rs.getLong("lab_id"),
                rs.getLong("at"),
                LabEvent.Kind.valueOf(rs.getString("kind")),
                rs.getString("message"));
    }
}
