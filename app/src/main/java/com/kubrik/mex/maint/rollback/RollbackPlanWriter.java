package com.kubrik.mex.maint.rollback;

import com.kubrik.mex.maint.model.RollbackPlan;
import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

/**
 * v2.7 Q2.7-A — Persists rollback plans attached to v2.4
 * {@code ops_audit} rows. The writer pre-checks that the audit row
 * exists (per §3.1 a FK would fight existing upgrade paths); the
 * responsibility moves here instead.
 *
 * <p>Plans are write-once. {@link #markApplied} records the outcome
 * of replay but never rewrites the original {@code plan_json} —
 * historical rollback intent stays recoverable even after a replay
 * that succeeds partially.</p>
 */
public final class RollbackPlanWriter {

    private final Database db;
    private final RollbackPlanDao dao;

    public RollbackPlanWriter(Database db) {
        this.db = db;
        this.dao = new RollbackPlanDao(db);
    }

    /** Persist a plan. Throws if the referenced audit row is missing —
     *  a rollback without its action context is useless and almost
     *  certainly a caller bug. Returns the new plan id. */
    public long write(RollbackPlan.Input in) {
        if (!auditRowExists(in.auditId())) {
            throw new IllegalStateException(
                    "ops_audit row " + in.auditId() + " does not exist — "
                    + "a rollback plan must attach to a real audit entry");
        }
        return dao.insert(in);
    }

    public Optional<RollbackPlan.Row> byId(long planId) { return dao.byId(planId); }

    /** All plans for a given audit row, ordered by insertion. A single
     *  action usually emits one plan, but composite flows (e.g.
     *  multi-step reconfig) may attach several. */
    public java.util.List<RollbackPlan.Row> byAuditId(long auditId) {
        return dao.byAuditId(auditId);
    }

    /** Mark a plan as applied with the outcome of the replay. Idempotent
     *  at the SQL level — a second call just updates the outcome +
     *  appends to the notes column so a retry history stays
     *  inspectable. */
    public boolean markApplied(long planId, RollbackPlan.Outcome outcome,
                                long appliedAt, String notes) {
        return dao.markApplied(planId, outcome, appliedAt, notes);
    }

    /* ============================ internals ============================ */

    private boolean auditRowExists(long auditId) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT 1 FROM ops_audit WHERE id = ?")) {
            ps.setLong(1, auditId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            // Don't mask DB failures as "bad audit id" — the caller
            // needs to distinguish a missing row from a locked DB so
            // operator sees the real cause in the status label.
            throw new RuntimeException(
                    "ops_audit lookup failed for id=" + auditId, e);
        }
    }
}
