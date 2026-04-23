package com.kubrik.mex.k8s.cluster;

import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-A4 — CRUD over {@code k8s_clusters}.
 *
 * <p>Writes are serialised on the shared SQLite write-lock (see
 * {@link Database#writeLock()}); reads run lock-free. The {@code
 * UNIQUE(kubeconfig_path, context_name)} constraint prevents dup rows
 * when a user "adds" the same context twice — callers handle the
 * SQLState {@code 23505} bounce as "update last_used_at on the
 * existing row."</p>
 */
public final class KubeClusterDao {

    private final Database database;

    public KubeClusterDao(Database database) {
        this.database = Objects.requireNonNull(database, "database");
    }

    /**
     * Insert a new row. Returns the generated id. Throws
     * {@link SQLException} (SQLState 23505) if the
     * {@code (kubeconfig_path, context_name)} pair is already present.
     */
    public long insert(String displayName, String kubeconfigPath, String contextName,
                       Optional<String> defaultNamespace, Optional<String> serverUrl)
            throws SQLException {
        synchronized (database.writeLock()) {
            String sql = "INSERT INTO k8s_clusters "
                    + "(display_name, kubeconfig_path, context_name, "
                    + " default_namespace, server_url, added_at) "
                    + "VALUES (?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = database.connection()
                    .prepareStatement(sql, java.sql.Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, displayName);
                ps.setString(2, kubeconfigPath);
                ps.setString(3, contextName);
                if (defaultNamespace.isPresent()) ps.setString(4, defaultNamespace.get());
                else ps.setNull(4, java.sql.Types.VARCHAR);
                if (serverUrl.isPresent()) ps.setString(5, serverUrl.get());
                else ps.setNull(5, java.sql.Types.VARCHAR);
                ps.setLong(6, System.currentTimeMillis());
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    return keys.next() ? keys.getLong(1) : -1L;
                }
            }
        }
    }

    public Optional<K8sClusterRef> findById(long id) throws SQLException {
        try (PreparedStatement ps = database.connection()
                .prepareStatement("SELECT * FROM k8s_clusters WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapRow(rs)) : Optional.empty();
            }
        }
    }

    public List<K8sClusterRef> listAll() throws SQLException {
        List<K8sClusterRef> out = new ArrayList<>();
        try (PreparedStatement ps = database.connection().prepareStatement(
                "SELECT * FROM k8s_clusters ORDER BY display_name ASC")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(mapRow(rs));
            }
        }
        return out;
    }

    /** Bump {@code last_used_at} after a successful probe or connect. */
    public void touch(long id) throws SQLException {
        synchronized (database.writeLock()) {
            try (PreparedStatement ps = database.connection().prepareStatement(
                    "UPDATE k8s_clusters SET last_used_at = ? WHERE id = ?")) {
                ps.setLong(1, System.currentTimeMillis());
                ps.setLong(2, id);
                ps.executeUpdate();
            }
        }
    }

    public void rename(long id, String displayName) throws SQLException {
        synchronized (database.writeLock()) {
            try (PreparedStatement ps = database.connection().prepareStatement(
                    "UPDATE k8s_clusters SET display_name = ? WHERE id = ?")) {
                ps.setString(1, displayName);
                ps.setLong(2, id);
                ps.executeUpdate();
            }
        }
    }

    public void updateDefaultNamespace(long id, Optional<String> ns) throws SQLException {
        synchronized (database.writeLock()) {
            try (PreparedStatement ps = database.connection().prepareStatement(
                    "UPDATE k8s_clusters SET default_namespace = ? WHERE id = ?")) {
                if (ns.isPresent()) ps.setString(1, ns.get());
                else ps.setNull(1, java.sql.Types.VARCHAR);
                ps.setLong(2, id);
                ps.executeUpdate();
            }
        }
    }

    /**
     * Remove a row. Will throw when SQLite's foreign-key RESTRICT
     * bounces the delete because a {@code provisioning_records} row
     * still points here.
     */
    public void delete(long id) throws SQLException {
        synchronized (database.writeLock()) {
            try (PreparedStatement ps = database.connection().prepareStatement(
                    "DELETE FROM k8s_clusters WHERE id = ?")) {
                ps.setLong(1, id);
                ps.executeUpdate();
            }
        }
    }

    /** Count live provisioning_records for a cluster (APPLYING or READY). */
    public int countLiveProvisions(long clusterId) throws SQLException {
        try (PreparedStatement ps = database.connection().prepareStatement(
                "SELECT COUNT(*) FROM provisioning_records "
                + "WHERE k8s_cluster_id = ? AND status IN ('APPLYING','READY')")) {
            ps.setLong(1, clusterId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }

    private static K8sClusterRef mapRow(ResultSet rs) throws SQLException {
        long lastUsed = rs.getLong("last_used_at");
        boolean lastUsedNull = rs.wasNull();
        String ns = rs.getString("default_namespace");
        String server = rs.getString("server_url");
        return new K8sClusterRef(
                rs.getLong("id"),
                rs.getString("display_name"),
                rs.getString("kubeconfig_path"),
                rs.getString("context_name"),
                Optional.ofNullable(ns),
                Optional.ofNullable(server),
                rs.getLong("added_at"),
                lastUsedNull ? Optional.empty() : Optional.of(lastUsed));
    }
}
