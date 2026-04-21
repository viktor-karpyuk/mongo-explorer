package com.kubrik.mex.cluster.store;

import com.kubrik.mex.cluster.model.TopologySnapshot;
import com.kubrik.mex.store.Database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * v2.4 TOPO-7 — persists {@link TopologySnapshot} rows with sha256-based
 * de-duplication. Callers invoke {@link #insertIfChanged} at their own cadence
 * (default 60 s per service tick); if the latest row for the connection carries
 * the same sha256 the insert is skipped.
 */
public final class TopologySnapshotDao {

    private final Database db;

    public TopologySnapshotDao(Database db) { this.db = db; }

    /** @return the row id on insert, or {@code -1} when the snapshot matched the latest sha256. */
    public synchronized long insertIfChanged(String connectionId, TopologySnapshot snap) {
        String latest = latestSha256(connectionId);
        String incoming = snap.sha256();
        if (incoming.equals(latest)) return -1L;
        return insert(connectionId, snap, incoming);
    }

    private long insert(String connectionId, TopologySnapshot snap, String sha256) {
        int[] mm = snap.majorMinor();
        String json = snap.toCanonicalJson();
        String sql = """
                INSERT INTO topology_snapshots (connection_id, captured_at, cluster_kind,
                    snapshot_json, sha256, version_major, version_minor, member_count, shard_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        try (PreparedStatement ps = db.connection().prepareStatement(sql,
                java.sql.Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, connectionId);
            ps.setLong(2, snap.capturedAt());
            ps.setString(3, snap.clusterKind().name());
            ps.setString(4, json);
            ps.setString(5, sha256);
            ps.setInt(6, mm[0]);
            ps.setInt(7, mm[1]);
            ps.setInt(8, snap.memberCount());
            ps.setInt(9, snap.shardCount());
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                return keys.next() ? keys.getLong(1) : -1L;
            }
        } catch (SQLException e) {
            // Unique constraint (connection_id, captured_at, sha256) → treat as duplicate.
            return -1L;
        }
    }

    public String latestSha256(String connectionId) {
        String sql = "SELECT sha256 FROM topology_snapshots WHERE connection_id = ? " +
                "ORDER BY captured_at DESC, id DESC LIMIT 1";
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setString(1, connectionId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (SQLException e) {
            return null;
        }
    }

    /** Test helper — returns rows newest first. */
    public List<Row> listRecent(String connectionId, int limit) {
        String sql = "SELECT id, captured_at, cluster_kind, snapshot_json, sha256, " +
                "member_count, shard_count FROM topology_snapshots " +
                "WHERE connection_id = ? ORDER BY captured_at DESC, id DESC LIMIT ?";
        List<Row> out = new ArrayList<>();
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setString(1, connectionId);
            ps.setInt(2, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new Row(rs.getLong("id"), rs.getLong("captured_at"),
                            rs.getString("cluster_kind"), rs.getString("snapshot_json"),
                            rs.getString("sha256"), rs.getInt("member_count"),
                            rs.getInt("shard_count")));
                }
            }
        } catch (SQLException e) {
            return List.of();
        }
        return out;
    }

    /** Cascade delete support — invoked when a connection row is deleted. */
    public void deleteForConnection(Connection conn, String connectionId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM topology_snapshots WHERE connection_id = ?")) {
            ps.setString(1, connectionId);
            ps.executeUpdate();
        }
    }

    public record Row(long id, long capturedAt, String clusterKind,
                      String snapshotJson, String sha256, int memberCount, int shardCount) {}
}
