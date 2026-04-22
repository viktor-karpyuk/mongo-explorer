package com.kubrik.mex.maint.schema;

import com.kubrik.mex.maint.model.ValidatorSpec;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 * v2.7 SCHV-4 — Applies a proposed validator via {@code collMod}.
 * The body mirrors what {@code db.collection.options()} would report
 * after the rollout, so a prior-state snapshot from
 * {@link ValidatorFetcher} is enough to build the rollback plan.
 */
public final class ValidatorRolloutRunner {

    public sealed interface Outcome {
        record Ok(int affectedDocs) implements Outcome {}
        record Failed(String code, String message) implements Outcome {}
    }

    public Outcome apply(MongoClient client, ValidatorSpec.Rollout rollout) {
        MongoDatabase db = client.getDatabase(rollout.db());
        Document validator = Document.parse(rollout.proposedValidatorJson());
        Document cmd = new Document("collMod", rollout.coll())
                .append("validator", validator)
                .append("validationLevel", levelToWire(rollout.level()))
                .append("validationAction", actionToWire(rollout.action()));
        try {
            Document reply = db.runCommand(cmd);
            double ok = reply.get("ok") instanceof Number n ? n.doubleValue() : 0.0;
            if (ok < 1.0) {
                return new Outcome.Failed(
                        reply.getString("codeName"),
                        reply.getString("errmsg"));
            }
            // collMod doesn't expose an affected-count; treat as
            // cluster-level metadata update with N/A.
            return new Outcome.Ok(0);
        } catch (MongoCommandException mce) {
            return new Outcome.Failed(mce.getErrorCodeName(),
                    mce.getErrorMessage());
        }
    }

    /** Build a {@code collMod} rollback command body from the pre-change
     *  snapshot. Used by {@code RollbackPlanWriter} — serialise and
     *  persist alongside the audit row. */
    public Document buildRollbackCommand(ValidatorSpec.Current prior) {
        return new Document("collMod", prior.coll())
                .append("validator", Document.parse(prior.validatorJson()))
                .append("validationLevel", levelToWire(prior.level()))
                .append("validationAction", actionToWire(prior.action()));
    }

    static String levelToWire(ValidatorSpec.Level level) {
        return switch (level) {
            case OFF -> "off";
            case MODERATE -> "moderate";
            case STRICT -> "strict";
        };
    }

    static String actionToWire(ValidatorSpec.Action action) {
        return action == ValidatorSpec.Action.WARN ? "warn" : "error";
    }
}
