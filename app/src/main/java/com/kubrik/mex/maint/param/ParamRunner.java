package com.kubrik.mex.maint.param;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Map;
import java.util.Optional;

/**
 * v2.7 Q2.7-F — Dispatches a single {@code setParameter} against
 * admin. The caller is responsible for reading the prior value via
 * {@link #get} + recording it in the rollback plan before dispatch.
 */
public final class ParamRunner {

    public sealed interface Outcome {
        record Ok(Object wasValue, Object nowValue) implements Outcome {}
        record Failed(String code, String message) implements Outcome {}
    }

    /** Reads a current parameter value. Returns empty on "no such
     *  parameter" — avoids a runner-level exception on a param name
     *  the server doesn't recognise (version skew, typo). */
    public Optional<Object> get(MongoClient client, String paramName) {
        MongoDatabase admin = client.getDatabase("admin");
        try {
            Document reply = admin.runCommand(new Document("getParameter", 1)
                    .append(paramName, 1));
            Object v = reply.get(paramName);
            return Optional.ofNullable(v);
        } catch (MongoCommandException mce) {
            return Optional.empty();
        }
    }

    /** Batch-read via a single getParameter(*). Falls back to per-param
     *  lookups if the server doesn't support the splat form. */
    public Map<String, Object> getAll(MongoClient client,
                                      java.util.Collection<String> names) {
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        for (String n : names) get(client, n).ifPresent(v -> out.put(n, v));
        return out;
    }

    public Outcome set(MongoClient client, String paramName, Object newValue) {
        MongoDatabase admin = client.getDatabase("admin");
        try {
            Document reply = admin.runCommand(new Document("setParameter", 1)
                    .append(paramName, newValue));
            Object was = reply.get("was");
            return new Outcome.Ok(was, newValue);
        } catch (MongoCommandException mce) {
            return new Outcome.Failed(mce.getErrorCodeName(),
                    mce.getErrorMessage());
        }
    }
}
