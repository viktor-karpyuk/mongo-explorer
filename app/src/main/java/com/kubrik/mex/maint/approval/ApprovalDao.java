package com.kubrik.mex.maint.approval;

import com.kubrik.mex.maint.model.Approval;
import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * v2.7 Q2.7-A — DAO for the {@code approvals} table. Pure SQL; the
 * {@link ApprovalService} owns the policy decisions (signature
 * verification, consumption semantics).
 */
public final class ApprovalDao {

    private final Database db;

    public ApprovalDao(Database db) { this.db = db; }

    public Approval.Row insert(Approval.Row r) {
        String sql = """
                INSERT INTO approvals(action_uuid, connection_id, action_name,
                    payload_json, payload_hash, requested_at, requested_by,
                    mode, approver, approved_at, approval_sig, status,
                    expires_at, reviewer_jws)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql,
                    Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, r.actionUuid());
                ps.setString(2, r.connectionId());
                ps.setString(3, r.actionName());
                ps.setString(4, r.payloadJson());
                ps.setString(5, r.payloadHash());
                ps.setLong(6, r.requestedAt());
                ps.setString(7, r.requestedBy());
                ps.setString(8, r.mode().name());
                if (r.approver() == null) ps.setNull(9, java.sql.Types.VARCHAR);
                else ps.setString(9, r.approver());
                if (r.approvedAt() == null) ps.setNull(10, java.sql.Types.INTEGER);
                else ps.setLong(10, r.approvedAt());
                if (r.approvalSig() == null) ps.setNull(11, java.sql.Types.VARCHAR);
                else ps.setString(11, r.approvalSig());
                ps.setString(12, r.status().name());
                if (r.expiresAt() == null) ps.setNull(13, java.sql.Types.INTEGER);
                else ps.setLong(13, r.expiresAt());
                if (r.reviewerJws() == null) ps.setNull(14, java.sql.Types.VARCHAR);
                else ps.setString(14, r.reviewerJws());
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    long id = keys.next() ? keys.getLong(1) : -1L;
                    return withId(r, id);
                }
            } catch (SQLException e) {
                throw new RuntimeException("approvals insert failed", e);
            }
        }
    }

    public Optional<Approval.Row> byActionUuid(String actionUuid) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM approvals WHERE action_uuid = ?")) {
            ps.setString(1, actionUuid);
            return first(ps);
        } catch (SQLException e) {
            return Optional.empty();
        }
    }

    public List<Approval.Row> listPending(String connectionId) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM approvals WHERE connection_id = ? AND status = 'PENDING' " +
                "ORDER BY requested_at DESC")) {
            ps.setString(1, connectionId);
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** Flip a PENDING row to APPROVED with the reviewer name + JWS
     *  signature attached. Refuses if the row is in any other state
     *  so a replay can't overwrite a CONSUMED approval. Also refuses
     *  if the row is past its expiry — the sweepExpired / approve
     *  race previously let an EXPIRED-eligible row slip through. */
    public boolean approve(String actionUuid, String approver, String approvalSig,
                           long approvedAt) {
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(
                    "UPDATE approvals SET approver = ?, approved_at = ?, " +
                    "approval_sig = ?, status = 'APPROVED' " +
                    "WHERE action_uuid = ? AND status = 'PENDING' " +
                    "AND (expires_at IS NULL OR expires_at > ?)")) {
                ps.setString(1, approver);
                ps.setLong(2, approvedAt);
                ps.setString(3, approvalSig);
                ps.setString(4, actionUuid);
                ps.setLong(5, approvedAt);
                return ps.executeUpdate() > 0;
            } catch (SQLException e) {
                return false;
            }
        }
    }

    public boolean reject(String actionUuid) {
        return transition(actionUuid, Approval.Status.PENDING, Approval.Status.REJECTED);
    }

    /** Mark an APPROVED row as CONSUMED. Callers should run this in
     *  the same transaction as the audit-row insert for the
     *  maintenance action; the service's markConsumed delegates here. */
    public boolean markConsumed(String actionUuid) {
        return transition(actionUuid, Approval.Status.APPROVED, Approval.Status.CONSUMED);
    }

    /** Sweep expired PENDING rows to EXPIRED. Returns the count swept;
     *  callers use it to surface a UI toast. */
    public int expireOverdue(long now) {
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(
                    "UPDATE approvals SET status = 'EXPIRED' " +
                    "WHERE status = 'PENDING' AND expires_at IS NOT NULL " +
                    "AND expires_at < ?")) {
                ps.setLong(1, now);
                return ps.executeUpdate();
            } catch (SQLException e) {
                return 0;
            }
        }
    }

    private boolean transition(String actionUuid,
                                Approval.Status from, Approval.Status to) {
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(
                    "UPDATE approvals SET status = ? " +
                    "WHERE action_uuid = ? AND status = ?")) {
                ps.setString(1, to.name());
                ps.setString(2, actionUuid);
                ps.setString(3, from.name());
                return ps.executeUpdate() > 0;
            } catch (SQLException e) {
                return false;
            }
        }
    }

    /* ============================ internals ============================ */

    private Optional<Approval.Row> first(PreparedStatement ps) throws SQLException {
        List<Approval.Row> rows = read(ps);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    private List<Approval.Row> read(PreparedStatement ps) throws SQLException {
        List<Approval.Row> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(map(rs));
        }
        return out;
    }

    private static Approval.Row map(ResultSet rs) throws SQLException {
        return new Approval.Row(
                rs.getLong("id"),
                rs.getString("action_uuid"),
                rs.getString("connection_id"),
                rs.getString("action_name"),
                rs.getString("payload_json"),
                rs.getString("payload_hash"),
                rs.getLong("requested_at"),
                rs.getString("requested_by"),
                Approval.Mode.valueOf(rs.getString("mode")),
                rs.getString("approver"),
                nullableLong(rs, "approved_at"),
                rs.getString("approval_sig"),
                Approval.Status.valueOf(rs.getString("status")),
                nullableLong(rs, "expires_at"),
                nullableString(rs, "reviewer_jws"));
    }

    private static Long nullableLong(ResultSet rs, String col) throws SQLException {
        long v = rs.getLong(col);
        return rs.wasNull() ? null : v;
    }

    /** reviewer_jws is a v2.7-review-GA additive column; older rows
     *  don't have it. Defend against a reader racing a migration. */
    private static String nullableString(ResultSet rs, String col) {
        try { return rs.getString(col); }
        catch (SQLException e) { return null; }
    }

    private static Approval.Row withId(Approval.Row r, long id) {
        return new Approval.Row(id, r.actionUuid(), r.connectionId(),
                r.actionName(), r.payloadJson(), r.payloadHash(),
                r.requestedAt(), r.requestedBy(), r.mode(), r.approver(),
                r.approvedAt(), r.approvalSig(), r.status(), r.expiresAt(),
                r.reviewerJws());
    }
}
