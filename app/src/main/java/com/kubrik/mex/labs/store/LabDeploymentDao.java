package com.kubrik.mex.labs.store;

import com.kubrik.mex.labs.model.LabDeployment;
import com.kubrik.mex.labs.model.LabStatus;
import com.kubrik.mex.labs.model.PortMap;
import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * v2.8.4 — DAO for {@code lab_deployments}. Plain SQL; the
 * LifecycleService owns state transitions.
 */
public final class LabDeploymentDao {

    private final Database db;

    public LabDeploymentDao(Database db) { this.db = db; }

    public LabDeployment insert(LabDeployment d) {
        String sql = """
                INSERT INTO lab_deployments(template_id, template_version,
                    display_name, compose_project, compose_file_path,
                    port_map_json, status, keep_data_on_stop, auth_enabled,
                    created_at, last_started_at, last_stopped_at,
                    destroyed_at, mongo_version, connection_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql,
                    Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, d.templateId());
                ps.setString(2, d.templateVersion());
                ps.setString(3, d.displayName());
                ps.setString(4, d.composeProject());
                ps.setString(5, d.composeFilePath());
                ps.setString(6, d.portMap().toJson());
                ps.setString(7, d.status().name());
                ps.setInt(8, d.keepDataOnStop() ? 1 : 0);
                ps.setInt(9, d.authEnabled() ? 1 : 0);
                ps.setLong(10, d.createdAt());
                setNullableLong(ps, 11, d.lastStartedAt());
                setNullableLong(ps, 12, d.lastStoppedAt());
                setNullableLong(ps, 13, d.destroyedAt());
                ps.setString(14, d.mongoImageTag());
                if (d.connectionId().isEmpty()) ps.setNull(15, java.sql.Types.VARCHAR);
                else ps.setString(15, d.connectionId().get());
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    long id = keys.next() ? keys.getLong(1) : -1L;
                    return withId(d, id);
                }
            } catch (SQLException e) {
                throw new RuntimeException("lab_deployments insert failed", e);
            }
        }
    }

    public Optional<LabDeployment> byId(long id) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM lab_deployments WHERE id = ?")) {
            ps.setLong(1, id);
            return first(ps);
        } catch (SQLException e) {
            return Optional.empty();
        }
    }

    public Optional<LabDeployment> byComposeProject(String project) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM lab_deployments WHERE compose_project = ?")) {
            ps.setString(1, project);
            return first(ps);
        } catch (SQLException e) {
            return Optional.empty();
        }
    }

    /** Every row NOT yet destroyed — used by the reconciler + Labs
     *  tab listing. Newest first. */
    public List<LabDeployment> listLive() {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM lab_deployments WHERE status != 'DESTROYED' " +
                "ORDER BY created_at DESC")) {
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    public boolean updateStatus(long id, LabStatus newStatus, Long timestampMs,
                                 String timestampColumn) {
        String sql = "UPDATE lab_deployments SET status = ?"
                + (timestampColumn == null ? "" : ", " + timestampColumn + " = ?")
                + " WHERE id = ?";
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
                ps.setString(1, newStatus.name());
                int idx = 2;
                if (timestampColumn != null) {
                    ps.setLong(idx++, timestampMs);
                }
                ps.setLong(idx, id);
                return ps.executeUpdate() > 0;
            } catch (SQLException e) {
                return false;
            }
        }
    }

    public boolean setConnectionId(long id, String connectionId) {
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(
                    "UPDATE lab_deployments SET connection_id = ? WHERE id = ?")) {
                ps.setString(1, connectionId);
                ps.setLong(2, id);
                return ps.executeUpdate() > 0;
            } catch (SQLException e) {
                return false;
            }
        }
    }

    /* ============================ internals ============================ */

    private static void setNullableLong(PreparedStatement ps, int idx,
                                         Optional<Long> v) throws SQLException {
        if (v.isEmpty()) ps.setNull(idx, java.sql.Types.INTEGER);
        else ps.setLong(idx, v.get());
    }

    private Optional<LabDeployment> first(PreparedStatement ps) throws SQLException {
        List<LabDeployment> rows = read(ps);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    private List<LabDeployment> read(PreparedStatement ps) throws SQLException {
        List<LabDeployment> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(map(rs));
        }
        return out;
    }

    private static LabDeployment map(ResultSet rs) throws SQLException {
        return new LabDeployment(
                rs.getLong("id"),
                rs.getString("template_id"),
                rs.getString("template_version"),
                rs.getString("display_name"),
                rs.getString("compose_project"),
                rs.getString("compose_file_path"),
                PortMap.fromJson(rs.getString("port_map_json")),
                LabStatus.valueOf(rs.getString("status")),
                rs.getInt("keep_data_on_stop") != 0,
                rs.getInt("auth_enabled") != 0,
                rs.getLong("created_at"),
                nullableLong(rs, "last_started_at"),
                nullableLong(rs, "last_stopped_at"),
                nullableLong(rs, "destroyed_at"),
                rs.getString("mongo_version"),
                Optional.ofNullable(rs.getString("connection_id")));
    }

    private static Optional<Long> nullableLong(ResultSet rs, String col) throws SQLException {
        long v = rs.getLong(col);
        return rs.wasNull() ? Optional.empty() : Optional.of(v);
    }

    private static LabDeployment withId(LabDeployment d, long id) {
        return new LabDeployment(id, d.templateId(), d.templateVersion(),
                d.displayName(), d.composeProject(), d.composeFilePath(),
                d.portMap(), d.status(), d.keepDataOnStop(),
                d.authEnabled(), d.createdAt(), d.lastStartedAt(),
                d.lastStoppedAt(), d.destroyedAt(), d.mongoImageTag(),
                d.connectionId());
    }
}
