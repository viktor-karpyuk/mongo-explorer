package com.kubrik.mex.backup.store;

import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * v2.5 BKP-RUN-1..8 — DAO for {@code backup_catalog}. Insert paths write a
 * RUNNING row up front so a crash-interrupted run stays visible; the finalise
 * update flips status + writes the manifest hash + roll-up metrics.
 */
public final class BackupCatalogDao {

    private final Database db;

    public BackupCatalogDao(Database db) { this.db = db; }

    public BackupCatalogRow insert(BackupCatalogRow r) {
        String sql = """
                INSERT INTO backup_catalog(policy_id, connection_id, started_at, finished_at,
                    status, sink_id, sink_path, manifest_sha256, total_bytes, doc_count,
                    oplog_first_ts, oplog_last_ts, verified_at, verify_outcome, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        try (PreparedStatement ps = db.connection().prepareStatement(sql,
                Statement.RETURN_GENERATED_KEYS)) {
            setNullableLong(ps, 1, r.policyId());
            ps.setString(2, r.connectionId());
            ps.setLong(3, r.startedAt());
            setNullableLong(ps, 4, r.finishedAt());
            ps.setString(5, r.status().name());
            ps.setLong(6, r.sinkId());
            ps.setString(7, r.sinkPath());
            setNullableString(ps, 8, r.manifestSha256());
            setNullableLong(ps, 9, r.totalBytes());
            setNullableLong(ps, 10, r.docCount());
            setNullableLong(ps, 11, r.oplogFirstTs());
            setNullableLong(ps, 12, r.oplogLastTs());
            setNullableLong(ps, 13, r.verifiedAt());
            setNullableString(ps, 14, r.verifyOutcome());
            setNullableString(ps, 15, r.notes());
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                long id = keys.next() ? keys.getLong(1) : -1L;
                return r.withId(id);
            }
        } catch (SQLException e) {
            throw new RuntimeException("backup_catalog insert failed", e);
        }
    }

    public void finalise(long id, BackupStatus status, long finishedAt,
                         String manifestSha256, Long totalBytes, Long docCount,
                         Long oplogFirstTs, Long oplogLastTs, String notes) {
        String sql = """
                UPDATE backup_catalog SET status = ?, finished_at = ?,
                    manifest_sha256 = ?, total_bytes = ?, doc_count = ?,
                    oplog_first_ts = ?, oplog_last_ts = ?, notes = ?
                WHERE id = ?
                """;
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setString(1, status.name());
            ps.setLong(2, finishedAt);
            setNullableString(ps, 3, manifestSha256);
            setNullableLong(ps, 4, totalBytes);
            setNullableLong(ps, 5, docCount);
            setNullableLong(ps, 6, oplogFirstTs);
            setNullableLong(ps, 7, oplogLastTs);
            setNullableString(ps, 8, notes);
            ps.setLong(9, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("backup_catalog finalise failed", e);
        }
    }

    public void recordVerification(long id, long verifiedAt, String outcome) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "UPDATE backup_catalog SET verified_at = ?, verify_outcome = ? WHERE id = ?")) {
            ps.setLong(1, verifiedAt);
            ps.setString(2, outcome);
            ps.setLong(3, id);
            ps.executeUpdate();
        } catch (SQLException ignored) {}
    }

    public Optional<BackupCatalogRow> byId(long id) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM backup_catalog WHERE id = ?")) {
            ps.setLong(1, id);
            return firstRow(ps);
        } catch (SQLException e) {
            return Optional.empty();
        }
    }

    public List<BackupCatalogRow> listForConnection(String connectionId, int limit) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM backup_catalog WHERE connection_id = ? " +
                        "ORDER BY started_at DESC, id DESC LIMIT ?")) {
            ps.setString(1, connectionId);
            ps.setInt(2, limit);
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    public List<BackupCatalogRow> listForPolicy(long policyId) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM backup_catalog WHERE policy_id = ? " +
                        "ORDER BY started_at DESC, id DESC")) {
            ps.setLong(1, policyId);
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** Rows for a policy with {@code started_at >= sinceMs}. Used by the
     *  scheduler's missed-runs backfill so a policy with a years-long history
     *  doesn't pull every row off disk just to compare against the 24h window. */
    public List<BackupCatalogRow> listForPolicySince(long policyId, long sinceMs) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM backup_catalog WHERE policy_id = ? AND started_at >= ? " +
                        "ORDER BY started_at DESC, id DESC")) {
            ps.setLong(1, policyId);
            ps.setLong(2, sinceMs);
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    public int deleteOlderThan(long cutoffMs) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "DELETE FROM backup_catalog WHERE finished_at IS NOT NULL AND finished_at < ?")) {
            ps.setLong(1, cutoffMs);
            return ps.executeUpdate();
        } catch (SQLException e) {
            return 0;
        }
    }

    /* ============================= internals ============================= */

    private Optional<BackupCatalogRow> firstRow(PreparedStatement ps) throws SQLException {
        List<BackupCatalogRow> rows = read(ps);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    private List<BackupCatalogRow> read(PreparedStatement ps) throws SQLException {
        List<BackupCatalogRow> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(map(rs));
        }
        return out;
    }

    private static BackupCatalogRow map(ResultSet rs) throws SQLException {
        long policyId = rs.getLong("policy_id");
        boolean policyNull = rs.wasNull();
        long finishedAt = rs.getLong("finished_at");
        boolean finishedNull = rs.wasNull();
        long totalBytes = rs.getLong("total_bytes");
        boolean totalNull = rs.wasNull();
        long docCount = rs.getLong("doc_count");
        boolean docNull = rs.wasNull();
        long oplogFirst = rs.getLong("oplog_first_ts");
        boolean oplogFirstNull = rs.wasNull();
        long oplogLast = rs.getLong("oplog_last_ts");
        boolean oplogLastNull = rs.wasNull();
        long verifiedAt = rs.getLong("verified_at");
        boolean verifiedNull = rs.wasNull();
        return new BackupCatalogRow(
                rs.getLong("id"),
                policyNull ? null : policyId,
                rs.getString("connection_id"),
                rs.getLong("started_at"),
                finishedNull ? null : finishedAt,
                BackupStatus.valueOf(rs.getString("status")),
                rs.getLong("sink_id"),
                rs.getString("sink_path"),
                rs.getString("manifest_sha256"),
                totalNull ? null : totalBytes,
                docNull ? null : docCount,
                oplogFirstNull ? null : oplogFirst,
                oplogLastNull ? null : oplogLast,
                verifiedNull ? null : verifiedAt,
                rs.getString("verify_outcome"),
                rs.getString("notes"));
    }

    private static void setNullableString(PreparedStatement ps, int idx, String v) throws SQLException {
        if (v == null) ps.setNull(idx, Types.VARCHAR); else ps.setString(idx, v);
    }

    private static void setNullableLong(PreparedStatement ps, int idx, Long v) throws SQLException {
        if (v == null) ps.setNull(idx, Types.BIGINT); else ps.setLong(idx, v);
    }
}
