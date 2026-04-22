package com.kubrik.mex.backup.store;

import com.kubrik.mex.backup.spec.BackupPolicy;
import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * v2.5 BKP-POLICY-1..7 — DAO for {@code backup_policies}. Scope, archive,
 * and retention are persisted as JSON blobs via
 * {@link BackupPolicyCodec}; the DAO never reaches inside those records so
 * evolving the spec only means updating the codec + the record.
 */
public final class BackupPolicyDao {

    private final Database db;

    public BackupPolicyDao(Database db) { this.db = db; }

    public BackupPolicy insert(BackupPolicy p) {
        String sql = """
                INSERT INTO backup_policies(connection_id, name, enabled, schedule_cron,
                    scope_json, archive_json, retention_json, sink_id, include_oplog,
                    created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        try (PreparedStatement ps = db.connection().prepareStatement(sql,
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, p.connectionId());
            ps.setString(2, p.name());
            ps.setInt(3, p.enabled() ? 1 : 0);
            if (p.scheduleCron() == null) ps.setNull(4, java.sql.Types.VARCHAR);
            else ps.setString(4, p.scheduleCron());
            ps.setString(5, BackupPolicyCodec.encodeScope(p.scope()));
            ps.setString(6, BackupPolicyCodec.encodeArchive(p.archive()));
            ps.setString(7, BackupPolicyCodec.encodeRetention(p.retention()));
            ps.setLong(8, p.sinkId());
            ps.setInt(9, p.includeOplog() ? 1 : 0);
            ps.setLong(10, p.createdAt());
            ps.setLong(11, p.updatedAt());
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                long id = keys.next() ? keys.getLong(1) : -1L;
                return p.withId(id);
            }
        } catch (SQLException e) {
            throw new RuntimeException("backup_policies insert failed", e);
        }
    }

    public Optional<BackupPolicy> byId(long id) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM backup_policies WHERE id = ?")) {
            ps.setLong(1, id);
            return firstRow(ps);
        } catch (SQLException e) {
            return Optional.empty();
        }
    }

    public List<BackupPolicy> listForConnection(String connectionId) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM backup_policies WHERE connection_id = ? ORDER BY name")) {
            ps.setString(1, connectionId);
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    public List<BackupPolicy> listEnabled() {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM backup_policies WHERE enabled = 1 ORDER BY connection_id, name")) {
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    public BackupPolicy update(BackupPolicy p) {
        String sql = """
                UPDATE backup_policies SET enabled = ?, schedule_cron = ?,
                    scope_json = ?, archive_json = ?, retention_json = ?,
                    sink_id = ?, include_oplog = ?, updated_at = ?
                WHERE id = ?
                """;
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setInt(1, p.enabled() ? 1 : 0);
            if (p.scheduleCron() == null) ps.setNull(2, java.sql.Types.VARCHAR);
            else ps.setString(2, p.scheduleCron());
            ps.setString(3, BackupPolicyCodec.encodeScope(p.scope()));
            ps.setString(4, BackupPolicyCodec.encodeArchive(p.archive()));
            ps.setString(5, BackupPolicyCodec.encodeRetention(p.retention()));
            ps.setLong(6, p.sinkId());
            ps.setInt(7, p.includeOplog() ? 1 : 0);
            ps.setLong(8, p.updatedAt());
            ps.setLong(9, p.id());
            ps.executeUpdate();
            return p;
        } catch (SQLException e) {
            throw new RuntimeException("backup_policies update failed", e);
        }
    }

    public boolean delete(long id) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "DELETE FROM backup_policies WHERE id = ?")) {
            ps.setLong(1, id);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            return false;
        }
    }

    /** Count of policies that reference a given sink. Used by
     *  SinksPane.onDelete as an application-level guard against
     *  orphaning policies on SQLite schemas created before the
     *  v2.6.1 FK landed. */
    public int countBySinkId(long sinkId) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT COUNT(*) FROM backup_policies WHERE sink_id = ?")) {
            ps.setLong(1, sinkId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (SQLException e) {
            return 0;
        }
    }

    /* ============================= internals ============================= */

    private Optional<BackupPolicy> firstRow(PreparedStatement ps) throws SQLException {
        List<BackupPolicy> rows = read(ps);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    private List<BackupPolicy> read(PreparedStatement ps) throws SQLException {
        List<BackupPolicy> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(map(rs));
        }
        return out;
    }

    private static BackupPolicy map(ResultSet rs) throws SQLException {
        return new BackupPolicy(
                rs.getLong("id"),
                rs.getString("connection_id"),
                rs.getString("name"),
                rs.getInt("enabled") != 0,
                rs.getString("schedule_cron"),
                BackupPolicyCodec.decodeScope(rs.getString("scope_json")),
                BackupPolicyCodec.decodeArchive(rs.getString("archive_json")),
                BackupPolicyCodec.decodeRetention(rs.getString("retention_json")),
                rs.getLong("sink_id"),
                rs.getInt("include_oplog") != 0,
                rs.getLong("created_at"),
                rs.getLong("updated_at"));
    }
}
