package com.kubrik.mex.maint.rollback;

import com.kubrik.mex.maint.model.RollbackPlan;
import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * v2.7 Q2.7-A — DAO for {@code rollback_plans}. Plain SQL; policy
 * lives with {@link RollbackPlanWriter}.
 */
public final class RollbackPlanDao {

    private final Database db;

    public RollbackPlanDao(Database db) { this.db = db; }

    public long insert(RollbackPlan.Input in) {
        String sql = """
                INSERT INTO rollback_plans(audit_id, plan_kind, plan_json, notes)
                VALUES (?, ?, ?, ?)
                """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql,
                    Statement.RETURN_GENERATED_KEYS)) {
                ps.setLong(1, in.auditId());
                ps.setString(2, in.kind().name());
                ps.setString(3, in.planJson());
                if (in.notes() == null) ps.setNull(4, java.sql.Types.VARCHAR);
                else ps.setString(4, in.notes());
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    return keys.next() ? keys.getLong(1) : -1L;
                }
            } catch (SQLException e) {
                throw new RuntimeException("rollback_plans insert failed", e);
            }
        }
    }

    public Optional<RollbackPlan.Row> byId(long id) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM rollback_plans WHERE id = ?")) {
            ps.setLong(1, id);
            return first(ps);
        } catch (SQLException e) {
            return Optional.empty();
        }
    }

    public List<RollbackPlan.Row> byAuditId(long auditId) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM rollback_plans WHERE audit_id = ? ORDER BY id")) {
            ps.setLong(1, auditId);
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    public boolean markApplied(long planId, RollbackPlan.Outcome outcome,
                                long appliedAt, String notes) {
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(
                    "UPDATE rollback_plans SET applied_at = ?, applied_outcome = ?, " +
                    "notes = COALESCE(notes, '') || CASE WHEN ? IS NULL THEN '' " +
                    "ELSE CASE WHEN notes IS NULL OR notes = '' THEN ? " +
                    "ELSE char(10) || ? END END " +
                    "WHERE id = ?")) {
                ps.setLong(1, appliedAt);
                ps.setString(2, outcome.name());
                ps.setString(3, notes);
                ps.setString(4, notes);
                ps.setString(5, notes);
                ps.setLong(6, planId);
                return ps.executeUpdate() > 0;
            } catch (SQLException e) {
                return false;
            }
        }
    }

    /* ============================ internals ============================ */

    private Optional<RollbackPlan.Row> first(PreparedStatement ps) throws SQLException {
        List<RollbackPlan.Row> rows = read(ps);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    private List<RollbackPlan.Row> read(PreparedStatement ps) throws SQLException {
        List<RollbackPlan.Row> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(map(rs));
        }
        return out;
    }

    private static RollbackPlan.Row map(ResultSet rs) throws SQLException {
        String outcomeStr = rs.getString("applied_outcome");
        Long appliedAt = rs.getLong("applied_at");
        if (rs.wasNull()) appliedAt = null;
        return new RollbackPlan.Row(
                rs.getLong("id"),
                rs.getLong("audit_id"),
                RollbackPlan.Kind.valueOf(rs.getString("plan_kind")),
                rs.getString("plan_json"),
                appliedAt,
                outcomeStr == null ? null : RollbackPlan.Outcome.valueOf(outcomeStr),
                rs.getString("notes"));
    }
}
