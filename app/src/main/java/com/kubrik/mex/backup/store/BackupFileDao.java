package com.kubrik.mex.backup.store;

import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * v2.5 BKP-RUN-5 — DAO for {@code backup_files}. The {@link #insertAll} helper
 * wraps a batch of rows for one catalog entry in a single transaction so the
 * manifest + file-row write is atomic: a crash mid-insert either leaves zero
 * file rows for that catalog id or the full set.
 */
public final class BackupFileDao {

    private final Database db;

    public BackupFileDao(Database db) { this.db = db; }

    public BackupFileRow insert(BackupFileRow r) {
        String sql = """
                INSERT INTO backup_files(catalog_id, relative_path, bytes, sha256, db, coll, kind)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;
        try (PreparedStatement ps = db.connection().prepareStatement(sql,
                Statement.RETURN_GENERATED_KEYS)) {
            bind(ps, r);
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                long id = keys.next() ? keys.getLong(1) : -1L;
                return r.withId(id);
            }
        } catch (SQLException e) {
            throw new RuntimeException("backup_files insert failed", e);
        }
    }

    public void insertAll(List<BackupFileRow> rows) {
        if (rows == null || rows.isEmpty()) return;
        String sql = """
                INSERT INTO backup_files(catalog_id, relative_path, bytes, sha256, db, coll, kind)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;
        java.sql.Connection c = db.connection();
        boolean prevAuto;
        try { prevAuto = c.getAutoCommit(); } catch (SQLException e) { prevAuto = true; }
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            c.setAutoCommit(false);
            for (BackupFileRow r : rows) {
                bind(ps, r);
                ps.addBatch();
            }
            ps.executeBatch();
            c.commit();
        } catch (SQLException e) {
            try { c.rollback(); } catch (SQLException ignored) {}
            throw new RuntimeException("backup_files batch insert failed", e);
        } finally {
            try { c.setAutoCommit(prevAuto); } catch (SQLException ignored) {}
        }
    }

    public List<BackupFileRow> listForCatalog(long catalogId) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM backup_files WHERE catalog_id = ? ORDER BY relative_path")) {
            ps.setLong(1, catalogId);
            List<BackupFileRow> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(map(rs));
            }
            return out;
        } catch (SQLException e) {
            return List.of();
        }
    }

    /* ============================= internals ============================= */

    private static void bind(PreparedStatement ps, BackupFileRow r) throws SQLException {
        ps.setLong(1, r.catalogId());
        ps.setString(2, r.relativePath());
        ps.setLong(3, r.bytes());
        ps.setString(4, r.sha256());
        if (r.db() == null) ps.setNull(5, Types.VARCHAR); else ps.setString(5, r.db());
        if (r.coll() == null) ps.setNull(6, Types.VARCHAR); else ps.setString(6, r.coll());
        ps.setString(7, r.kind());
    }

    private static BackupFileRow map(ResultSet rs) throws SQLException {
        return new BackupFileRow(
                rs.getLong("id"),
                rs.getLong("catalog_id"),
                rs.getString("relative_path"),
                rs.getLong("bytes"),
                rs.getString("sha256"),
                rs.getString("db"),
                rs.getString("coll"),
                rs.getString("kind"));
    }
}
