package com.kubrik.mex.maint.drift;

import com.kubrik.mex.maint.model.ConfigSnapshot;
import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * v2.7 DRIFT-CFG — Persistence for the config-snapshot subsystem.
 */
public final class ConfigSnapshotDao {

    private final Database db;

    public ConfigSnapshotDao(Database db) { this.db = db; }

    public long insert(ConfigSnapshot snap) {
        String sql = """
                INSERT INTO config_snapshots(connection_id, captured_at, host,
                    kind, snapshot_json, sha256)
                VALUES (?, ?, ?, ?, ?, ?)
                """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql,
                    Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, snap.connectionId());
                ps.setLong(2, snap.capturedAt());
                if (snap.host() == null) ps.setNull(3, java.sql.Types.VARCHAR);
                else ps.setString(3, snap.host());
                ps.setString(4, snap.kind().name());
                ps.setString(5, snap.snapshotJson());
                ps.setString(6, snap.sha256());
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    return keys.next() ? keys.getLong(1) : -1L;
                }
            } catch (SQLException e) {
                throw new RuntimeException("config_snapshots insert failed", e);
            }
        }
    }

    public List<ConfigSnapshot> listForConnection(String connectionId, int limit) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM config_snapshots WHERE connection_id = ? " +
                "ORDER BY captured_at DESC LIMIT ?")) {
            ps.setString(1, connectionId);
            ps.setInt(2, limit);
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** Latest snapshot of a kind for a connection + host (null host =
     *  cluster-wide). */
    public Optional<ConfigSnapshot> latest(String connectionId,
                                           String host,
                                           ConfigSnapshot.Kind kind) {
        String whereHost = host == null ? "host IS NULL" : "host = ?";
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM config_snapshots WHERE connection_id = ? AND "
                + whereHost + " AND kind = ? ORDER BY captured_at DESC LIMIT 1")) {
            int idx = 1;
            ps.setString(idx++, connectionId);
            if (host != null) ps.setString(idx++, host);
            ps.setString(idx, kind.name());
            List<ConfigSnapshot> rows = read(ps);
            return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
        } catch (SQLException e) {
            return Optional.empty();
        }
    }

    /** SHA-256 over the canonical JSON bytes — the same hash the v2.6
     *  DriftDiffEngine keys on, so two equivalent snapshots collapse
     *  into one. */
    public static String sha256(String canonicalJson) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(canonicalJson.getBytes(
                    java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (NoSuchAlgorithmException nsa) {
            throw new IllegalStateException("SHA-256 unavailable", nsa);
        }
    }

    private List<ConfigSnapshot> read(PreparedStatement ps) throws SQLException {
        List<ConfigSnapshot> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(map(rs));
        }
        return out;
    }

    private static ConfigSnapshot map(ResultSet rs) throws SQLException {
        return new ConfigSnapshot(
                rs.getLong("id"),
                rs.getString("connection_id"),
                rs.getLong("captured_at"),
                rs.getString("host"),
                ConfigSnapshot.Kind.valueOf(rs.getString("kind")),
                rs.getString("snapshot_json"),
                rs.getString("sha256"));
    }
}
