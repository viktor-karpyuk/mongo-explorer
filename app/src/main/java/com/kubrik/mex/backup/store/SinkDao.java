package com.kubrik.mex.backup.store;

import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * v2.5 STG-1..5 — DAO for {@code storage_sinks}.
 *
 * <p>Credentials are always in ciphertext at rest. Callers pass plain-text
 * {@code credentialsJson} to {@link #insert}; the DAO routes through
 * {@link Crypto#encrypt(String)} (AES-GCM, per-install key). Reads decrypt
 * back to plain text so callers never touch raw ciphertext.</p>
 */
public final class SinkDao {

    private final Database db;
    private final Crypto crypto;

    public SinkDao(Database db, Crypto crypto) {
        this.db = db;
        this.crypto = crypto;
    }

    public SinkRecord insert(SinkRecord r) {
        String sql = """
                INSERT INTO storage_sinks(kind, name, root_path, credentials_enc,
                    extras_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;
        try (PreparedStatement ps = db.connection().prepareStatement(sql,
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, r.kind());
            ps.setString(2, r.name());
            ps.setString(3, r.rootPath());
            String enc = crypto.encrypt(r.credentialsJson());
            if (enc == null) ps.setNull(4, java.sql.Types.BLOB);
            else ps.setBytes(4, enc.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            if (r.extrasJson() == null) ps.setNull(5, java.sql.Types.VARCHAR);
            else ps.setString(5, r.extrasJson());
            ps.setLong(6, r.createdAt());
            ps.setLong(7, r.updatedAt());
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                long id = keys.next() ? keys.getLong(1) : -1L;
                return r.withId(id);
            }
        } catch (SQLException e) {
            throw new RuntimeException("storage_sinks insert failed", e);
        }
    }

    public Optional<SinkRecord> byId(long id) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM storage_sinks WHERE id = ?")) {
            ps.setLong(1, id);
            return firstRow(ps);
        } catch (SQLException e) {
            return Optional.empty();
        }
    }

    public Optional<SinkRecord> byName(String name) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM storage_sinks WHERE name = ?")) {
            ps.setString(1, name);
            return firstRow(ps);
        } catch (SQLException e) {
            return Optional.empty();
        }
    }

    public List<SinkRecord> listAll() {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM storage_sinks ORDER BY name")) {
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** Full-field update. Treats {@code credentialsJson == null} as "clear" —
     *  call {@link #byId} + re-insert if you want to preserve the existing
     *  ciphertext while editing other fields. */
    public SinkRecord update(SinkRecord r) {
        String sql = """
                UPDATE storage_sinks SET kind = ?, name = ?, root_path = ?,
                    credentials_enc = ?, extras_json = ?, updated_at = ?
                WHERE id = ?
                """;
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setString(1, r.kind());
            ps.setString(2, r.name());
            ps.setString(3, r.rootPath());
            String enc = crypto.encrypt(r.credentialsJson());
            if (enc == null) ps.setNull(4, java.sql.Types.BLOB);
            else ps.setBytes(4, enc.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            if (r.extrasJson() == null) ps.setNull(5, java.sql.Types.VARCHAR);
            else ps.setString(5, r.extrasJson());
            ps.setLong(6, r.updatedAt());
            ps.setLong(7, r.id());
            ps.executeUpdate();
            return r;
        } catch (SQLException e) {
            throw new RuntimeException("storage_sinks update failed", e);
        }
    }

    public boolean delete(long id) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "DELETE FROM storage_sinks WHERE id = ?")) {
            ps.setLong(1, id);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            return false;
        }
    }

    /* ============================= internals ============================= */

    private Optional<SinkRecord> firstRow(PreparedStatement ps) throws SQLException {
        List<SinkRecord> rows = read(ps);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    private List<SinkRecord> read(PreparedStatement ps) throws SQLException {
        List<SinkRecord> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(map(rs));
        }
        return out;
    }

    private SinkRecord map(ResultSet rs) throws SQLException {
        byte[] enc = rs.getBytes("credentials_enc");
        String decoded = enc == null ? null
                : crypto.decrypt(new String(enc, java.nio.charset.StandardCharsets.UTF_8));
        return new SinkRecord(
                rs.getLong("id"),
                rs.getString("kind"),
                rs.getString("name"),
                rs.getString("root_path"),
                decoded,
                rs.getString("extras_json"),
                rs.getLong("created_at"),
                rs.getLong("updated_at"));
    }
}
