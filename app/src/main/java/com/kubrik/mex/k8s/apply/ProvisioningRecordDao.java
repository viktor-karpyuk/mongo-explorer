package com.kubrik.mex.k8s.apply;

import com.kubrik.mex.k8s.model.ProvisioningRecord;
import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-H — CRUD over {@code provisioning_records}.
 *
 * <p>One row per provision attempt. {@link #insertApplying} stamps
 * the row at Apply start; {@link #markStatus} flips it through the
 * lifecycle. {@link #attachConnection} back-links the auto-created
 * Mongo Explorer connection once the rollout succeeds.</p>
 */
public final class ProvisioningRecordDao {

    private final Database database;

    public ProvisioningRecordDao(Database database) {
        this.database = Objects.requireNonNull(database, "database");
    }

    public long insertApplying(long clusterId, String namespace, String name,
                                 String operator, String operatorVersion, String mongoVersion,
                                 String topology, String profile,
                                 String crYaml, String crSha256, boolean deletionProtection)
            throws SQLException {
        synchronized (database.writeLock()) {
            String sql = "INSERT INTO provisioning_records("
                    + "k8s_cluster_id, namespace, name, operator, operator_version, "
                    + "mongo_version, topology, profile, cr_yaml, cr_sha256, "
                    + "deletion_protection, created_at, status) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'APPLYING')";
            try (PreparedStatement ps = database.connection()
                    .prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                ps.setLong(1, clusterId);
                ps.setString(2, namespace);
                ps.setString(3, name);
                ps.setString(4, operator);
                ps.setString(5, operatorVersion);
                ps.setString(6, mongoVersion);
                ps.setString(7, topology);
                ps.setString(8, profile);
                ps.setString(9, crYaml);
                ps.setString(10, crSha256);
                ps.setInt(11, deletionProtection ? 1 : 0);
                ps.setLong(12, System.currentTimeMillis());
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    return keys.next() ? keys.getLong(1) : -1L;
                }
            }
        }
    }

    public void markStatus(long rowId, ProvisioningRecord.Status status) throws SQLException {
        synchronized (database.writeLock()) {
            String sql = status == ProvisioningRecord.Status.READY
                    ? "UPDATE provisioning_records SET status = ?, applied_at = ? WHERE id = ?"
                    : "UPDATE provisioning_records SET status = ? WHERE id = ?";
            try (PreparedStatement ps = database.connection().prepareStatement(sql)) {
                ps.setString(1, status.name());
                if (status == ProvisioningRecord.Status.READY) {
                    ps.setLong(2, System.currentTimeMillis());
                    ps.setLong(3, rowId);
                } else {
                    ps.setLong(2, rowId);
                }
                ps.executeUpdate();
            }
        }
    }

    public void attachConnection(long rowId, String connectionId) throws SQLException {
        synchronized (database.writeLock()) {
            try (PreparedStatement ps = database.connection().prepareStatement(
                    "UPDATE provisioning_records SET connection_id = ? WHERE id = ?")) {
                ps.setString(1, connectionId);
                ps.setLong(2, rowId);
                ps.executeUpdate();
            }
        }
    }

    /**
     * v2.8.1 Q2.8.1-I — Flip the deletion-protection bit.
     *
     * <p>TearDownService's first-gate call lives here rather than on
     * the service so the update goes through {@link Database#writeLock}
     * alongside every other write. {@code true} re-enables protection
     * (e.g. re-locking a Prod row after an aborted delete).</p>
     */
    public void setDeletionProtection(long rowId, boolean enabled) throws SQLException {
        synchronized (database.writeLock()) {
            try (PreparedStatement ps = database.connection().prepareStatement(
                    "UPDATE provisioning_records SET deletion_protection = ? WHERE id = ?")) {
                ps.setInt(1, enabled ? 1 : 0);
                ps.setLong(2, rowId);
                ps.executeUpdate();
            }
        }
    }

    public Optional<ProvisioningRecord> findById(long id) throws SQLException {
        try (PreparedStatement ps = database.connection().prepareStatement(
                "SELECT * FROM provisioning_records WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(map(rs)) : Optional.empty();
            }
        }
    }

    public List<ProvisioningRecord> listAll() throws SQLException {
        List<ProvisioningRecord> out = new ArrayList<>();
        try (PreparedStatement ps = database.connection().prepareStatement(
                "SELECT * FROM provisioning_records ORDER BY created_at DESC");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(map(rs));
        }
        return out;
    }

    public void delete(long id) throws SQLException {
        synchronized (database.writeLock()) {
            try (PreparedStatement ps = database.connection().prepareStatement(
                    "DELETE FROM provisioning_records WHERE id = ?")) {
                ps.setLong(1, id);
                ps.executeUpdate();
            }
        }
    }

    private static ProvisioningRecord map(ResultSet rs) throws SQLException {
        long applied = rs.getLong("applied_at");
        boolean appliedNull = rs.wasNull();
        String connId = rs.getString("connection_id");
        return new ProvisioningRecord(
                rs.getLong("id"),
                rs.getLong("k8s_cluster_id"),
                rs.getString("namespace"),
                rs.getString("name"),
                rs.getString("operator"),
                rs.getString("operator_version"),
                rs.getString("mongo_version"),
                rs.getString("topology"),
                rs.getString("profile"),
                rs.getString("cr_yaml"),
                rs.getString("cr_sha256"),
                rs.getInt("deletion_protection") != 0,
                rs.getLong("created_at"),
                appliedNull ? Optional.empty() : Optional.of(applied),
                ProvisioningRecord.Status.valueOf(rs.getString("status")),
                Optional.ofNullable(connId));
    }
}
