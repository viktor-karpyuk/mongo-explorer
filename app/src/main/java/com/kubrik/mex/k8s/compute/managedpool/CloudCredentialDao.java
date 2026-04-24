package com.kubrik.mex.k8s.compute.managedpool;

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
 * v2.8.4 Q2.8.4-A — CRUD over {@code cloud_credentials}.
 *
 * <p>Every write goes through {@link Database#writeLock()} to serialise
 * concurrent inserts / probes with the rest of the app's DAO traffic.
 * Secret bodies are out of scope here — {@link CloudCredential#keychainRef}
 * is the only pointer; the UI + adapter resolve the actual credential
 * material against the OS keychain at use-time.</p>
 */
public final class CloudCredentialDao {

    private final Database database;

    public CloudCredentialDao(Database database) {
        this.database = Objects.requireNonNull(database, "database");
    }

    public long insert(String displayName, CloudProvider provider,
                        CloudCredential.AuthMode authMode, String keychainRef,
                        Optional<String> awsAccountId, Optional<String> gcpProject,
                        Optional<String> azureSubscription, Optional<String> defaultRegion)
            throws SQLException {
        synchronized (database.writeLock()) {
            String sql = "INSERT INTO cloud_credentials(display_name, provider, auth_mode, "
                    + "keychain_ref, aws_account_id, gcp_project, azure_subscription, "
                    + "default_region, created_at) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = database.connection()
                    .prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, displayName);
                ps.setString(2, provider.wireValue());
                ps.setString(3, authMode.name());
                ps.setString(4, keychainRef);
                setOptional(ps, 5, awsAccountId);
                setOptional(ps, 6, gcpProject);
                setOptional(ps, 7, azureSubscription);
                setOptional(ps, 8, defaultRegion);
                ps.setLong(9, System.currentTimeMillis());
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    return keys.next() ? keys.getLong(1) : -1L;
                }
            }
        }
    }

    public void recordProbe(long id, CloudCredential.ProbeStatus status) throws SQLException {
        synchronized (database.writeLock()) {
            try (PreparedStatement ps = database.connection().prepareStatement(
                    "UPDATE cloud_credentials SET last_probed_at = ?, probe_status = ? "
                    + "WHERE id = ?")) {
                ps.setLong(1, System.currentTimeMillis());
                ps.setString(2, status.name());
                ps.setLong(3, id);
                ps.executeUpdate();
            }
        }
    }

    public Optional<CloudCredential> findById(long id) throws SQLException {
        try (PreparedStatement ps = database.connection().prepareStatement(
                "SELECT * FROM cloud_credentials WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapRow(rs)) : Optional.empty();
            }
        }
    }

    public List<CloudCredential> listAll() throws SQLException {
        List<CloudCredential> out = new ArrayList<>();
        try (PreparedStatement ps = database.connection().prepareStatement(
                "SELECT * FROM cloud_credentials ORDER BY created_at DESC");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(mapRow(rs));
        }
        return out;
    }

    public List<CloudCredential> listByProvider(CloudProvider provider) throws SQLException {
        List<CloudCredential> out = new ArrayList<>();
        try (PreparedStatement ps = database.connection().prepareStatement(
                "SELECT * FROM cloud_credentials WHERE provider = ? ORDER BY created_at DESC")) {
            ps.setString(1, provider.wireValue());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(mapRow(rs));
            }
        }
        return out;
    }

    public void delete(long id) throws SQLException {
        synchronized (database.writeLock()) {
            try (PreparedStatement ps = database.connection().prepareStatement(
                    "DELETE FROM cloud_credentials WHERE id = ?")) {
                ps.setLong(1, id);
                ps.executeUpdate();
            }
        }
    }

    /* ============================ internals ============================ */

    private static void setOptional(PreparedStatement ps, int idx, Optional<String> v)
            throws SQLException {
        if (v.isPresent()) ps.setString(idx, v.get());
        else ps.setNull(idx, java.sql.Types.VARCHAR);
    }

    private static CloudCredential mapRow(ResultSet rs) throws SQLException {
        long probed = rs.getLong("last_probed_at");
        boolean probedNull = rs.wasNull();
        String probeStatusRaw = rs.getString("probe_status");
        return new CloudCredential(
                rs.getLong("id"),
                rs.getString("display_name"),
                CloudProvider.fromWire(rs.getString("provider")),
                CloudCredential.AuthMode.valueOf(rs.getString("auth_mode")),
                rs.getString("keychain_ref"),
                Optional.ofNullable(rs.getString("aws_account_id")),
                Optional.ofNullable(rs.getString("gcp_project")),
                Optional.ofNullable(rs.getString("azure_subscription")),
                Optional.ofNullable(rs.getString("default_region")),
                rs.getLong("created_at"),
                probedNull ? Optional.empty() : Optional.of(probed),
                probeStatusRaw == null ? Optional.empty()
                        : Optional.of(CloudCredential.ProbeStatus.valueOf(probeStatusRaw)));
    }
}
