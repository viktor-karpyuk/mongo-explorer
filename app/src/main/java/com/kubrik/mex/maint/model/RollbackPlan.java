package com.kubrik.mex.maint.model;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.7 Q2.7-A — A persisted rollback plan attached to a v2.4
 * {@code ops_audit} row. Each destructive maintenance action that can
 * be reversed (reconfig, validator, param, index drop) emits one plan
 * in the same transaction as the audit insert; the replay service
 * opens the matching wizard pre-filled with the inverse spec.
 *
 * <p>Plans are write-once — to modify a plan, write a new row that
 * references the updated audit entry. This matches milestone §4.4
 * ("Plans are not editable once written") and keeps the rollback
 * history immutable for auditors.</p>
 *
 * <p>Not every action gets a plan. Compact is naturally reversible
 * (it's a no-op if re-run); data migrations produce too large a
 * prior-state blob to store here. {@link Kind} enumerates the
 * currently-planned kinds.</p>
 */
public final class RollbackPlan {

    public enum Kind { RS_CONFIG, VALIDATOR, PARAM, INDEX_DROP }

    public enum Outcome { OK, FAIL, NA }

    public record Row(
            long id,
            long auditId,
            Kind kind,
            String planJson,
            Long appliedAt,
            Outcome appliedOutcome,
            String notes
    ) {
        public Row {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(planJson, "planJson");
            if (planJson.isBlank())
                throw new IllegalArgumentException("planJson is blank");
        }

        public Optional<Outcome> outcome() {
            return Optional.ofNullable(appliedOutcome);
        }
    }

    /** Input to the writer — lacks the id + applied-at fields that are
     *  set on persist / replay. */
    public record Input(long auditId, Kind kind, String planJson, String notes) {
        public Input {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(planJson, "planJson");
            if (planJson.isBlank())
                throw new IllegalArgumentException("planJson is blank");
        }
    }

    private RollbackPlan() {}
}
