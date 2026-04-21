package com.kubrik.mex.security.cis;

import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * v2.6 Q2.6-H3 — DAO for {@code cis_suppressions}. Writes suppressions
 * with an optional TTL (the typical "auditor-approved exception for
 * the next 90 days" shape) and reads them back filtered by connection
 * + active-at-timestamp, which is what the runner consumes.
 */
public final class CisSuppressionsDao {

    private final Database db;

    public CisSuppressionsDao(Database db) { this.db = db; }

    public CisSuppression insert(CisSuppression s) {
        String sql = """
                INSERT INTO cis_suppressions(connection_id, rule_id, scope, reason,
                    created_at, created_by, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql,
                    Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, s.connectionId());
                ps.setString(2, s.ruleId());
                ps.setString(3, s.scope());
                ps.setString(4, s.reason());
                ps.setLong(5, s.createdAtMs());
                ps.setString(6, s.createdBy());
                if (s.expiresAtMs() == null) ps.setNull(7, Types.BIGINT);
                else ps.setLong(7, s.expiresAtMs());
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    long id = keys.next() ? keys.getLong(1) : -1L;
                    return new CisSuppression(id, s.connectionId(), s.ruleId(),
                            s.scope(), s.reason(), s.createdAtMs(),
                            s.createdBy(), s.expiresAtMs());
                }
            } catch (SQLException e) {
                throw new RuntimeException("cis_suppressions insert failed", e);
            }
        }
    }

    /** Active = no expiry or expiry beyond {@code atMs}. */
    public List<CisSuppression> listActive(String connectionId, long atMs) {
        String sql = "SELECT * FROM cis_suppressions WHERE connection_id = ? " +
                "AND (expires_at IS NULL OR expires_at > ?) " +
                "ORDER BY created_at DESC";
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setString(1, connectionId);
            ps.setLong(2, atMs);
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    public List<CisSuppression> listAll(String connectionId) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM cis_suppressions WHERE connection_id = ? " +
                        "ORDER BY created_at DESC")) {
            ps.setString(1, connectionId);
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    public boolean delete(long id) {
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(
                    "DELETE FROM cis_suppressions WHERE id = ?")) {
                ps.setLong(1, id);
                return ps.executeUpdate() > 0;
            } catch (SQLException e) {
                return false;
            }
        }
    }

    /* ============================== mapping =============================== */

    private List<CisSuppression> read(PreparedStatement ps) throws SQLException {
        List<CisSuppression> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(map(rs));
        }
        return out;
    }

    private static CisSuppression map(ResultSet rs) throws SQLException {
        Long expires = rs.getLong("expires_at");
        if (rs.wasNull()) expires = null;
        return new CisSuppression(
                rs.getLong("id"),
                rs.getString("connection_id"),
                rs.getString("rule_id"),
                rs.getString("scope"),
                rs.getString("reason"),
                rs.getLong("created_at"),
                rs.getString("created_by"),
                expires);
    }
}
