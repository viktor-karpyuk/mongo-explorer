package com.kubrik.mex.labs.k8s.store;

import com.kubrik.mex.labs.k8s.model.LabK8sCluster;
import com.kubrik.mex.labs.k8s.model.LabK8sClusterStatus;
import com.kubrik.mex.labs.k8s.model.LabK8sDistro;
import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8-N3 — CRUD over {@code lab_k8s_clusters}.
 *
 * <p>Writes go through {@link Database#writeLock} so multi-threaded
 * creation / destruction can't interleave their INSERT / UPDATE /
 * DELETE statements — matches the discipline every other Lab DAO
 * in the codebase follows.</p>
 */
public final class LabK8sClusterDao {

    private final Database database;

    public LabK8sClusterDao(Database database) {
        this.database = Objects.requireNonNull(database, "database");
    }

    public long insertCreating(LabK8sDistro distro, String identifier,
                                 String contextName, String kubeconfigPath) throws SQLException {
        synchronized (database.writeLock()) {
            String sql = "INSERT INTO lab_k8s_clusters("
                    + "distro, identifier, context_name, kubeconfig_path, status, created_at) "
                    + "VALUES (?, ?, ?, ?, 'CREATING', ?)";
            try (PreparedStatement ps = database.connection().prepareStatement(
                    sql, Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, distro.name());
                ps.setString(2, identifier);
                ps.setString(3, contextName);
                ps.setString(4, kubeconfigPath);
                ps.setLong(5, System.currentTimeMillis());
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    return keys.next() ? keys.getLong(1) : -1L;
                }
            }
        }
    }

    public void updateStatus(long id, LabK8sClusterStatus status,
                              long at, String timestampColumn) throws SQLException {
        String safeColumn = switch (timestampColumn) {
            case "last_started_at", "last_stopped_at", "destroyed_at" -> timestampColumn;
            default -> throw new IllegalArgumentException("unknown ts column: " + timestampColumn);
        };
        synchronized (database.writeLock()) {
            String sql = "UPDATE lab_k8s_clusters SET status = ?, " + safeColumn
                    + " = ? WHERE id = ?";
            try (PreparedStatement ps = database.connection().prepareStatement(sql)) {
                ps.setString(1, status.name());
                ps.setLong(2, at);
                ps.setLong(3, id);
                ps.executeUpdate();
            }
        }
    }

    public void attachK8sCluster(long id, long k8sClusterId) throws SQLException {
        synchronized (database.writeLock()) {
            try (PreparedStatement ps = database.connection().prepareStatement(
                    "UPDATE lab_k8s_clusters SET k8s_cluster_id = ? WHERE id = ?")) {
                ps.setLong(1, k8sClusterId);
                ps.setLong(2, id);
                ps.executeUpdate();
            }
        }
    }

    public Optional<LabK8sCluster> findById(long id) throws SQLException {
        try (PreparedStatement ps = database.connection().prepareStatement(
                "SELECT * FROM lab_k8s_clusters WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapRow(rs)) : Optional.empty();
            }
        }
    }

    public Optional<LabK8sCluster> findByIdentifier(LabK8sDistro distro, String identifier) throws SQLException {
        try (PreparedStatement ps = database.connection().prepareStatement(
                "SELECT * FROM lab_k8s_clusters WHERE distro = ? AND identifier = ?")) {
            ps.setString(1, distro.name());
            ps.setString(2, identifier);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapRow(rs)) : Optional.empty();
            }
        }
    }

    public List<LabK8sCluster> listLive() throws SQLException {
        List<LabK8sCluster> out = new ArrayList<>();
        try (PreparedStatement ps = database.connection().prepareStatement(
                "SELECT * FROM lab_k8s_clusters WHERE status != 'DESTROYED' "
                + "ORDER BY created_at DESC")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(mapRow(rs));
            }
        }
        return out;
    }

    /** Q2.8-N6 — map of mongo connection id → Lab K8s cluster, for any
     *  live (non-destroyed) Lab whose production provisioning record
     *  has a materialised mongo connection. Powers the ConnectionTree
     *  "Lab•K8s" chip. */
    public Map<String, LabK8sCluster> connectionIdToCluster() throws SQLException {
        Map<String, LabK8sCluster> out = new HashMap<>();
        try (PreparedStatement ps = database.connection().prepareStatement(
                "SELECT p.connection_id, l.* FROM provisioning_records p "
              + "JOIN lab_k8s_clusters l ON p.lab_k8s_cluster_id = l.id "
              + "WHERE p.connection_id IS NOT NULL AND l.status != 'DESTROYED'")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.put(rs.getString("connection_id"), mapRow(rs));
                }
            }
        }
        return out;
    }

    public List<LabK8sCluster> listAll() throws SQLException {
        List<LabK8sCluster> out = new ArrayList<>();
        try (PreparedStatement ps = database.connection().prepareStatement(
                "SELECT * FROM lab_k8s_clusters ORDER BY created_at DESC");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(mapRow(rs));
        }
        return out;
    }

    private static LabK8sCluster mapRow(ResultSet rs) throws SQLException {
        long lastStart = rs.getLong("last_started_at");
        boolean lastStartNull = rs.wasNull();
        long lastStop = rs.getLong("last_stopped_at");
        boolean lastStopNull = rs.wasNull();
        long destroyed = rs.getLong("destroyed_at");
        boolean destroyedNull = rs.wasNull();
        long k8sClusterId = rs.getLong("k8s_cluster_id");
        boolean k8sNull = rs.wasNull();
        return new LabK8sCluster(
                rs.getLong("id"),
                LabK8sDistro.valueOf(rs.getString("distro")),
                rs.getString("identifier"),
                rs.getString("context_name"),
                rs.getString("kubeconfig_path"),
                LabK8sClusterStatus.valueOf(rs.getString("status")),
                rs.getLong("created_at"),
                lastStartNull ? Optional.empty() : Optional.of(lastStart),
                lastStopNull ? Optional.empty() : Optional.of(lastStop),
                destroyedNull ? Optional.empty() : Optional.of(destroyed),
                k8sNull ? Optional.empty() : Optional.of(k8sClusterId));
    }
}
