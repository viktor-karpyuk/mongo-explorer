package com.kubrik.mex.maint.compact;

import com.kubrik.mex.maint.model.CompactSpec;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 * v2.7 CMPT-2 — Triggers a secondary resync via {@code resync}.
 *
 * <p>The resync command reaches "done" only after the node has caught
 * up on its oplog; we don't block on that here (could be hours).
 * Callers wanting progress tail {@code replSetGetStatus} through the
 * existing monitoring pipeline.</p>
 */
public final class ResyncRunner {

    public sealed interface Outcome {
        record Ok() implements Outcome {}
        record PrimaryRefused() implements Outcome {}
        record Failed(String code, String message) implements Outcome {}
    }

    public Outcome run(MongoClient targetMember, CompactSpec.Resync spec) {
        MongoDatabase admin = targetMember.getDatabase("admin");
        Document hello = admin.runCommand(new Document("hello", 1));
        if (hello.getBoolean("isWritablePrimary", false)) {
            return new Outcome.PrimaryRefused();
        }
        try {
            // resync has been deprecated in favour of
            // initialSyncStandalone in recent server versions but
            // still accepted for backward-compat. The wizard doc
            // explains the behaviour per version.
            admin.runCommand(new Document("resync", 1));
            return new Outcome.Ok();
        } catch (Exception e) {
            return new Outcome.Failed(e.getClass().getSimpleName(),
                    e.getMessage());
        }
    }
}
