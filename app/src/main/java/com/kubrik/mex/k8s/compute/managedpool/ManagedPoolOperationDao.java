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
 * v2.8.4 Q2.8.4-F — CRUD over {@code managed_pool_operations}.
 *
 * <p>Every cloud API call that runs on behalf of a provisioning row
 * lands here so the {@code cloud.*} audit prefix is round-trippable
 * with CloudTrail / GCP Audit Logs / Azure Activity Logs via the
 * {@code cloud_call_id} column.</p>
 */
public final class ManagedPoolOperationDao {

    public enum Action {
        POOL_CREATE, POOL_DESCRIBE, POOL_DELETE, AUTH_PROBE;

        public String wireValue() { return "cloud." + name().toLowerCase().replace('_', '.'); }
    }

    public enum Status { ACCEPTED, REJECTED, OK, FAILED }

    public record Row(
            long id,
            Optional<Long> provisioningRecordId,
            CloudProvider provider,
            Action action,
            Optional<String> region,
            Optional<String> accountId,
            Optional<String> poolName,
            Optional<String> cloudCallId,
            long startedAt,
            Optional<Long> endedAt,
            Status status,
            Optional<String> errorMessage
    ) {}

    private final Database database;

    public ManagedPoolOperationDao(Database database) {
        this.database = Objects.requireNonNull(database, "database");
    }

    public long start(Optional<Long> provisioningRecordId, CloudProvider provider, Action action,
                       Optional<String> region, Optional<String> accountId,
                       Optional<String> poolName) throws SQLException {
        synchronized (database.writeLock()) {
            String sql = "INSERT INTO managed_pool_operations("
                    + "provisioning_record_id, provider, action, region, account_id, "
                    + "pool_name, started_at, status) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, 'ACCEPTED')";
            try (PreparedStatement ps = database.connection()
                    .prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                if (provisioningRecordId.isPresent()) ps.setLong(1, provisioningRecordId.get());
                else ps.setNull(1, java.sql.Types.INTEGER);
                ps.setString(2, provider.wireValue());
                ps.setString(3, action.wireValue());
                setOptional(ps, 4, region);
                setOptional(ps, 5, accountId);
                setOptional(ps, 6, poolName);
                ps.setLong(7, System.currentTimeMillis());
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    return keys.next() ? keys.getLong(1) : -1L;
                }
            }
        }
    }

    public void finish(long rowId, Status status, Optional<String> cloudCallId,
                        Optional<String> errorMessage) throws SQLException {
        synchronized (database.writeLock()) {
            try (PreparedStatement ps = database.connection().prepareStatement(
                    "UPDATE managed_pool_operations SET ended_at = ?, status = ?, "
                    + "cloud_call_id = COALESCE(cloud_call_id, ?), "
                    + "error_message = ? WHERE id = ?")) {
                ps.setLong(1, System.currentTimeMillis());
                ps.setString(2, status.name());
                if (cloudCallId.isPresent()) ps.setString(3, cloudCallId.get());
                else ps.setNull(3, java.sql.Types.VARCHAR);
                if (errorMessage.isPresent()) ps.setString(4, errorMessage.get());
                else ps.setNull(4, java.sql.Types.VARCHAR);
                ps.setLong(5, rowId);
                ps.executeUpdate();
            }
        }
    }

    public List<Row> listForProvision(long provisioningRecordId) throws SQLException {
        List<Row> out = new ArrayList<>();
        try (PreparedStatement ps = database.connection().prepareStatement(
                "SELECT * FROM managed_pool_operations "
                + "WHERE provisioning_record_id = ? ORDER BY started_at ASC")) {
            ps.setLong(1, provisioningRecordId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(mapRow(rs));
            }
        }
        return out;
    }

    public List<Row> listAll() throws SQLException {
        List<Row> out = new ArrayList<>();
        try (PreparedStatement ps = database.connection().prepareStatement(
                "SELECT * FROM managed_pool_operations ORDER BY started_at DESC LIMIT 200");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(mapRow(rs));
        }
        return out;
    }

    /* ============================ internals ============================ */

    private static void setOptional(PreparedStatement ps, int idx, Optional<String> v)
            throws SQLException {
        if (v.isPresent()) ps.setString(idx, v.get());
        else ps.setNull(idx, java.sql.Types.VARCHAR);
    }

    private static Row mapRow(ResultSet rs) throws SQLException {
        long ended = rs.getLong("ended_at");
        boolean endedNull = rs.wasNull();
        long provId = rs.getLong("provisioning_record_id");
        boolean provNull = rs.wasNull();
        return new Row(
                rs.getLong("id"),
                provNull ? Optional.empty() : Optional.of(provId),
                CloudProvider.fromWire(rs.getString("provider")),
                actionFromWire(rs.getString("action")),
                Optional.ofNullable(rs.getString("region")),
                Optional.ofNullable(rs.getString("account_id")),
                Optional.ofNullable(rs.getString("pool_name")),
                Optional.ofNullable(rs.getString("cloud_call_id")),
                rs.getLong("started_at"),
                endedNull ? Optional.empty() : Optional.of(ended),
                Status.valueOf(rs.getString("status")),
                Optional.ofNullable(rs.getString("error_message")));
    }

    private static Action actionFromWire(String raw) {
        if (raw == null) return Action.POOL_DESCRIBE;
        String tail = raw.startsWith("cloud.") ? raw.substring(6) : raw;
        return switch (tail) {
            case "pool.create" -> Action.POOL_CREATE;
            case "pool.delete" -> Action.POOL_DELETE;
            case "pool.describe" -> Action.POOL_DESCRIBE;
            case "auth.probe" -> Action.AUTH_PROBE;
            default -> Action.POOL_DESCRIBE;
        };
    }
}
