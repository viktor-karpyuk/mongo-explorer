package com.kubrik.mex.maint.rollback;

import com.kubrik.mex.maint.model.RollbackPlan;

import java.util.Optional;

/**
 * v2.7 — Given a rollback-plan id, hands the caller enough context to
 * re-open the matching wizard pre-filled with the inverse spec.
 *
 * <p>Deliberately thin — the FX panes know how to render themselves
 * given a {@link ReplayRequest}. This class just looks up the plan,
 * verifies it hasn't already been replayed to completion, and
 * returns the typed payload + {@link RollbackPlan.Kind} so the UI
 * can route to the right wizard.</p>
 */
public final class RollbackReplayService {

    private final RollbackPlanWriter writer;

    public RollbackReplayService(RollbackPlanWriter writer) {
        this.writer = writer;
    }

    /** Context the wizard needs to open with the rollback pre-filled.
     *  {@code planJson} is the stored plan body — the wizard deserialises
     *  back into its typed form (e.g. a prior rs.conf document). */
    public record ReplayRequest(
            long planId,
            RollbackPlan.Kind kind,
            String planJson,
            boolean alreadyApplied
    ) {}

    public Optional<ReplayRequest> lookup(long planId) {
        return writer.byId(planId).map(row -> new ReplayRequest(
                row.id(), row.kind(), row.planJson(),
                row.appliedAt() != null));
    }

    /** Convenience for the audit drawer: find the primary plan for an
     *  audit row. Audit rows most often have exactly one plan; when
     *  multiple exist the first by insertion order wins. */
    public Optional<ReplayRequest> lookupByAuditId(long auditId) {
        return writer.byAuditId(auditId).stream()
                .findFirst()
                .map(row -> new ReplayRequest(row.id(), row.kind(),
                        row.planJson(), row.appliedAt() != null));
    }

    /** Record the replay outcome after the wizard's runner has fired.
     *  Sugar over {@link RollbackPlanWriter#markApplied}. */
    public boolean recordOutcome(long planId, RollbackPlan.Outcome outcome,
                                  String notes) {
        return writer.markApplied(planId, outcome,
                System.currentTimeMillis(), notes);
    }
}
