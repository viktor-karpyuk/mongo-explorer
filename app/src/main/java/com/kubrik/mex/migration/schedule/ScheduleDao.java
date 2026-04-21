package com.kubrik.mex.migration.schedule;

import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** CRUD over {@code migration_schedules} (UX-7). */
public final class ScheduleDao {

    private final Database db;

    public ScheduleDao(Database db) { this.db = db; }

    public void upsert(MigrationSchedule s) throws SQLException {
        String sql = """
                INSERT INTO migration_schedules
                  (id, profile_id, cron, enabled, last_run_at, next_run_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  profile_id  = excluded.profile_id,
                  cron        = excluded.cron,
                  enabled     = excluded.enabled,
                  last_run_at = excluded.last_run_at,
                  next_run_at = excluded.next_run_at
                """;
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setString(1, s.id());
            ps.setString(2, s.profileId());
            ps.setString(3, s.expression());
            ps.setInt   (4, s.enabled() ? 1 : 0);
            if (s.lastRunAtMs() != null) ps.setLong(5, s.lastRunAtMs()); else ps.setNull(5, Types.INTEGER);
            if (s.nextRunAtMs() != null) ps.setLong(6, s.nextRunAtMs()); else ps.setNull(6, Types.INTEGER);
            ps.setLong  (7, s.createdAtMs());
            ps.executeUpdate();
        }
    }

    public void delete(String id) throws SQLException {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "DELETE FROM migration_schedules WHERE id = ?")) {
            ps.setString(1, id);
            ps.executeUpdate();
        }
    }

    public Optional<MigrationSchedule> find(String id) throws SQLException {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM migration_schedules WHERE id = ?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(from(rs)) : Optional.empty();
            }
        }
    }

    /** Every enabled row whose {@code next_run_at} has elapsed (null = never-run, eligible). */
    public List<MigrationSchedule> dueAt(long nowMs) throws SQLException {
        List<MigrationSchedule> out = new ArrayList<>();
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM migration_schedules WHERE enabled = 1 " +
                        "AND (next_run_at IS NULL OR next_run_at <= ?)")) {
            ps.setLong(1, nowMs);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(from(rs));
            }
        }
        return out;
    }

    public List<MigrationSchedule> listAll() throws SQLException {
        List<MigrationSchedule> out = new ArrayList<>();
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM migration_schedules ORDER BY created_at DESC")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(from(rs));
            }
        }
        return out;
    }

    public void markRun(String id, long lastRunAtMs, long nextRunAtMs) throws SQLException {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "UPDATE migration_schedules SET last_run_at = ?, next_run_at = ? WHERE id = ?")) {
            ps.setLong(1, lastRunAtMs);
            ps.setLong(2, nextRunAtMs);
            ps.setString(3, id);
            ps.executeUpdate();
        }
    }

    private static MigrationSchedule from(ResultSet rs) throws SQLException {
        long lastRun = rs.getLong("last_run_at");
        boolean lastRunNull = rs.wasNull();
        long nextRun = rs.getLong("next_run_at");
        boolean nextRunNull = rs.wasNull();
        return new MigrationSchedule(
                rs.getString("id"),
                rs.getString("profile_id"),
                rs.getString("cron"),
                rs.getInt("enabled") != 0,
                lastRunNull ? null : lastRun,
                nextRunNull ? null : nextRun,
                rs.getLong("created_at"));
    }
}
