package com.kubrik.mex.migration.schedule;

import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** CRUD over {@code migration_schedules}. Partner to {@link com.kubrik.mex.migration.store.ProfileStore}
 *  — a schedule points at one profile by id and carries the cron expression that fires it. */
public final class ScheduleStore {

    private final Database db;

    public ScheduleStore(Database db) { this.db = db; }

    public Schedule create(String profileId, String cron, boolean enabled, Instant nextRunAt) {
        String id = UUID.randomUUID().toString();
        long now = Instant.now().toEpochMilli();
        String sql = """
            INSERT INTO migration_schedules (id, profile_id, cron, enabled, next_run_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """;
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setString(1, id);
            ps.setString(2, profileId);
            ps.setString(3, cron);
            ps.setInt(4, enabled ? 1 : 0);
            setLongOrNull(ps, 5, nextRunAt == null ? null : nextRunAt.toEpochMilli());
            ps.setLong(6, now);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("create schedule failed: " + e.getMessage(), e);
        }
        return new Schedule(id, profileId, cron, enabled, null, nextRunAt,
                Instant.ofEpochMilli(now));
    }

    /** Marks a schedule enabled/disabled without touching its timing fields. */
    public void setEnabled(String id, boolean enabled) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "UPDATE migration_schedules SET enabled = ? WHERE id = ?")) {
            ps.setInt(1, enabled ? 1 : 0);
            ps.setString(2, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("setEnabled failed: " + e.getMessage(), e);
        }
    }

    /** Records a fire-and-advance: writes {@code last_run_at} to now and moves
     *  {@code next_run_at} forward to the cron-resolved time. */
    public void recordRun(String id, Instant lastRunAt, Instant nextRunAt) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "UPDATE migration_schedules SET last_run_at = ?, next_run_at = ? WHERE id = ?")) {
            setLongOrNull(ps, 1, lastRunAt == null ? null : lastRunAt.toEpochMilli());
            setLongOrNull(ps, 2, nextRunAt == null ? null : nextRunAt.toEpochMilli());
            ps.setString(3, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("recordRun failed: " + e.getMessage(), e);
        }
    }

    public void delete(String id) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "DELETE FROM migration_schedules WHERE id = ?")) {
            ps.setString(1, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("delete schedule failed: " + e.getMessage(), e);
        }
    }

    public Optional<Schedule> get(String id) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM migration_schedules WHERE id = ?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(map(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException("get schedule failed: " + e.getMessage(), e);
        }
    }

    public List<Schedule> list() {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM migration_schedules ORDER BY created_at");
             ResultSet rs = ps.executeQuery()) {
            List<Schedule> out = new ArrayList<>();
            while (rs.next()) out.add(map(rs));
            return out;
        } catch (SQLException e) {
            throw new RuntimeException("list schedules failed: " + e.getMessage(), e);
        }
    }

    /** Returns enabled schedules whose {@code next_run_at} is at or before {@code asOf}. The
     *  background scheduler calls this every tick — the composite index
     *  {@code idx_migration_schedules_next_run} ensures the scan stays cheap as schedules grow. */
    public List<Schedule> listDue(Instant asOf) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM migration_schedules WHERE enabled = 1 AND next_run_at IS NOT NULL "
              + "AND next_run_at <= ? ORDER BY next_run_at")) {
            ps.setLong(1, asOf.toEpochMilli());
            try (ResultSet rs = ps.executeQuery()) {
                List<Schedule> out = new ArrayList<>();
                while (rs.next()) out.add(map(rs));
                return out;
            }
        } catch (SQLException e) {
            throw new RuntimeException("listDue failed: " + e.getMessage(), e);
        }
    }

    // --- helpers -----------------------------------------------------------------

    private static void setLongOrNull(PreparedStatement ps, int ix, Long v) throws SQLException {
        if (v == null) ps.setNull(ix, Types.INTEGER);
        else ps.setLong(ix, v);
    }

    private static Schedule map(ResultSet rs) throws SQLException {
        return new Schedule(
                rs.getString("id"),
                rs.getString("profile_id"),
                rs.getString("cron"),
                rs.getInt("enabled") == 1,
                instantOrNull(rs, "last_run_at"),
                instantOrNull(rs, "next_run_at"),
                Instant.ofEpochMilli(rs.getLong("created_at")));
    }

    private static Instant instantOrNull(ResultSet rs, String column) throws SQLException {
        long v = rs.getLong(column);
        return rs.wasNull() ? null : Instant.ofEpochMilli(v);
    }
}
