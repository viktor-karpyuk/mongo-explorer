package com.kubrik.mex.maint.compact;

import com.kubrik.mex.maint.model.CompactSpec;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * v2.7 CMPT-1 — Runs {@code compact} against a chosen secondary.
 *
 * <p>The primary-refusal guard is the caller's responsibility (the
 * wizard knows the topology); this class enforces it again server-
 * side by checking {@code hello.isWritablePrimary} before dispatch.
 * Belt + braces; a stale topology snapshot shouldn't cause a compact
 * on a primary.</p>
 */
public final class CompactRunner {

    public record CollectionOutcome(
            String db, String coll, boolean success,
            String errorCode, String errorMessage, long elapsedMs) {}

    public record Result(
            boolean primaryRefused,
            List<CollectionOutcome> perCollection,
            long totalElapsedMs) {}

    public Result run(MongoClient targetMember, CompactSpec.Compact spec) {
        long t0 = System.currentTimeMillis();
        // Guard — last-line defence against a stale topology.
        MongoDatabase admin = targetMember.getDatabase("admin");
        Document hello = admin.runCommand(new Document("hello", 1));
        if (hello.getBoolean("isWritablePrimary", false)) {
            return new Result(true, List.of(),
                    System.currentTimeMillis() - t0);
        }

        MongoDatabase db = targetMember.getDatabase(spec.db());
        List<CollectionOutcome> outcomes = new ArrayList<>(spec.collections().size());
        for (String coll : spec.collections()) {
            long ts = System.currentTimeMillis();
            try {
                Document cmd = new Document("compact", coll);
                if (spec.force()) cmd.append("force", true);
                db.runCommand(cmd);
                outcomes.add(new CollectionOutcome(spec.db(), coll, true,
                        null, null, System.currentTimeMillis() - ts));
            } catch (Exception e) {
                outcomes.add(new CollectionOutcome(spec.db(), coll, false,
                        e.getClass().getSimpleName(), e.getMessage(),
                        System.currentTimeMillis() - ts));
            }
        }
        return new Result(false, List.copyOf(outcomes),
                System.currentTimeMillis() - t0);
    }

    /** CMPT-1 helper — client-side primary refusal, used by the wizard
     *  when the user picks a target. Checking here as well as in
     *  {@link #run} means the bad path never opens an SSH/SRV client. */
    public static boolean wouldTargetPrimary(String targetHost,
                                             String primaryHost) {
        return targetHost.equals(primaryHost);
    }
}
