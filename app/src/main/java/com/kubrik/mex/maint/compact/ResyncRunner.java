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
        record NotSupported(String serverVersion) implements Outcome {}
        record Failed(String code, String message) implements Outcome {}
    }

    public Outcome run(MongoClient targetMember, CompactSpec.Resync spec) {
        MongoDatabase admin = targetMember.getDatabase("admin");
        Document hello = admin.runCommand(new Document("hello", 1));
        if (hello.getBoolean("isWritablePrimary", false)) {
            return new Outcome.PrimaryRefused();
        }

        // The `resync` command was removed in MongoDB 5.0. Calling it
        // returns CommandNotFound (code 59) — which was previously
        // surfaced as a generic Failed. For a clear operator message,
        // gate the dispatch on the server version and return
        // NotSupported with guidance to use the dbpath-wipe flow.
        String version = readServerVersion(admin);
        int major = parseMajor(version);
        if (major >= 5) {
            return new Outcome.NotSupported(version);
        }

        try {
            admin.runCommand(new Document("resync", 1));
            return new Outcome.Ok();
        } catch (Exception e) {
            return new Outcome.Failed(e.getClass().getSimpleName(),
                    e.getMessage());
        }
    }

    private static String readServerVersion(MongoDatabase admin) {
        try {
            Document info = admin.runCommand(new Document("buildInfo", 1));
            String v = info.getString("version");
            return v == null ? "unknown" : v;
        } catch (Exception e) {
            return "unknown";
        }
    }

    static int parseMajor(String version) {
        if (version == null || version.isBlank()) return 0;
        int dot = version.indexOf('.');
        String head = dot < 0 ? version : version.substring(0, dot);
        try { return Integer.parseInt(head.replaceAll("[^0-9]", "")); }
        catch (NumberFormatException nfe) { return 0; }
    }
}
