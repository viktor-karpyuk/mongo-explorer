package com.kubrik.mex.security.baseline;

import com.kubrik.mex.cluster.dryrun.CommandJson;
import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * v2.6 Q2.6-A3 — DAO for {@code sec_baselines}. Persists the canonical
 * JSON of a {@link SecurityBaseline} alongside its SHA-256 so the drift
 * engine can short-circuit on equal hashes.
 *
 * <p>Parsing the stored JSON back into a nested {@code Map} is intentional-
 * ly not provided here: Q2.6-D diffs over the JSON text directly rather
 * than through reconstructed Java objects, which keeps the structural
 * comparison stable across schema changes to the payload shape.</p>
 */
public final class SecurityBaselineDao {

    private final Database db;

    public SecurityBaselineDao(Database db) { this.db = db; }

    public record Row(
            long id,
            String connectionId,
            long capturedAt,
            String capturedBy,
            String notes,
            String snapshotJson,
            String sha256
    ) {}

    public Row insert(SecurityBaseline b) {
        String json = b.canonicalJson();
        String hash = b.sha256();
        String sql = """
                INSERT INTO sec_baselines(connection_id, captured_at, captured_by,
                    notes, snapshot_json, sha256)
                VALUES (?, ?, ?, ?, ?, ?)
                """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql,
                    Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, b.connectionId());
                ps.setLong(2, b.capturedAtMs());
                ps.setString(3, b.capturedBy());
                ps.setString(4, b.notes());
                ps.setString(5, json);
                ps.setString(6, hash);
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    long id = keys.next() ? keys.getLong(1) : -1L;
                    return new Row(id, b.connectionId(), b.capturedAtMs(),
                            b.capturedBy(), b.notes(), json, hash);
                }
            } catch (SQLException e) {
                throw new RuntimeException("sec_baselines insert failed", e);
            }
        }
    }

    public Optional<Row> byId(long id) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM sec_baselines WHERE id = ?")) {
            ps.setLong(1, id);
            return firstRow(ps);
        } catch (SQLException e) {
            return Optional.empty();
        }
    }

    /** Most-recent first. Limit keeps the UI list bounded on long-lived installs. */
    public List<Row> listForConnection(String connectionId, int limit) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM sec_baselines WHERE connection_id = ? " +
                        "ORDER BY captured_at DESC, id DESC LIMIT ?")) {
            ps.setString(1, connectionId);
            ps.setInt(2, limit);
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    public Optional<Row> latestForConnection(String connectionId) {
        List<Row> rows = listForConnection(connectionId, 1);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    public int delete(long id) {
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(
                    "DELETE FROM sec_baselines WHERE id = ?")) {
                ps.setLong(1, id);
                return ps.executeUpdate();
            } catch (SQLException e) {
                return 0;
            }
        }
    }

    /* ============================= internals ============================= */

    private Optional<Row> firstRow(PreparedStatement ps) throws SQLException {
        List<Row> rows = read(ps);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    private List<Row> read(PreparedStatement ps) throws SQLException {
        List<Row> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(map(rs));
        }
        return out;
    }

    private static Row map(ResultSet rs) throws SQLException {
        return new Row(
                rs.getLong("id"),
                rs.getString("connection_id"),
                rs.getLong("captured_at"),
                rs.getString("captured_by"),
                rs.getString("notes"),
                rs.getString("snapshot_json"),
                rs.getString("sha256"));
    }

    /** Helper for callers that captured a nested map but haven't wrapped it
     *  into a {@link SecurityBaseline} yet — common during Q2.6-B's probe
     *  tests. Produces the same canonical JSON the record would. */
    public static String canonicalise(Map<String, Object> payload) {
        return CommandJson.render(payload == null ? Map.of() : payload);
    }
}
