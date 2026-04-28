package com.kubrik.mex.maint.schema;

import com.kubrik.mex.maint.model.ValidatorSpec;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.util.Optional;

/**
 * v2.7 SCHV-2 — Fetches the current {@code $jsonSchema} validator
 * from a target collection via {@code listCollections}.
 *
 * <p>{@code listCollections} returns the {@code options} block the
 * collection was created with (or the current state if modified);
 * we pull {@code validator}, {@code validationLevel}, and
 * {@code validationAction} from there. Falls back to an empty
 * validator if the collection has none.</p>
 */
public final class ValidatorFetcher {

    public Optional<ValidatorSpec.Current> fetch(MongoClient client,
                                                 String db, String coll) {
        MongoDatabase database = client.getDatabase(db);
        for (Document info : database.listCollections()
                .filter(Filters.eq("name", coll))
                .batchSize(1)) {
            Document options = info.get("options", Document.class);
            String validatorJson = "{}";
            ValidatorSpec.Level level = ValidatorSpec.Level.OFF;
            ValidatorSpec.Action action = ValidatorSpec.Action.ERROR;
            if (options != null) {
                Document validator = options.get("validator", Document.class);
                if (validator != null) {
                    validatorJson = validator.toJson(
                            JsonWriterSettings.builder().indent(true).build());
                }
                String levelStr = options.getString("validationLevel");
                if (levelStr != null) level = parseLevel(levelStr);
                String actionStr = options.getString("validationAction");
                if (actionStr != null) action = parseAction(actionStr);
            }
            return Optional.of(new ValidatorSpec.Current(db, coll,
                    validatorJson, level, action));
        }
        return Optional.empty();
    }

    static ValidatorSpec.Level parseLevel(String s) {
        return switch (s.toLowerCase()) {
            case "strict" -> ValidatorSpec.Level.STRICT;
            case "moderate" -> ValidatorSpec.Level.MODERATE;
            default -> ValidatorSpec.Level.OFF;
        };
    }

    static ValidatorSpec.Action parseAction(String s) {
        return "warn".equalsIgnoreCase(s)
                ? ValidatorSpec.Action.WARN
                : ValidatorSpec.Action.ERROR;
    }
}
