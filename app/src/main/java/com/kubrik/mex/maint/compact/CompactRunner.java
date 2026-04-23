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

    /** Default per-collection compact timeout — 1 hour. Compact on a
     *  large collection can legitimately run for minutes, but without
     *  an upper bound the UI would hang indefinitely on a stuck call. */
    public static final long DEFAULT_MAX_TIME_MS = 60L * 60L * 1000L;

    private final long maxTimeMs;

    public CompactRunner() { this(DEFAULT_MAX_TIME_MS); }

    public CompactRunner(long maxTimeMs) {
        this.maxTimeMs = maxTimeMs;
    }

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
                Document cmd = new Document("compact", coll)
                        .append("maxTimeMS", maxTimeMs);
                if (spec.force()) cmd.append("force", true);
                db.runCommand(cmd);
                outcomes.add(new CollectionOutcome(spec.db(), coll, true,
                        null, null, System.currentTimeMillis() - ts));
            } catch (com.mongodb.MongoExecutionTimeoutException te) {
                outcomes.add(new CollectionOutcome(spec.db(), coll, false,
                        "MaxTimeMSExpired",
                        "compact exceeded maxTimeMS=" + maxTimeMs + "ms",
                        System.currentTimeMillis() - ts));
            } catch (com.mongodb.MongoCommandException mce) {
                outcomes.add(new CollectionOutcome(spec.db(), coll, false,
                        mce.getErrorCodeName(), mce.getErrorMessage(),
                        System.currentTimeMillis() - ts));
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
     *  {@link #run} means the bad path never opens an SSH/SRV client.
     *
     *  <p>Compares hostnames case-insensitively and normalizes missing
     *  ports to 27017 so a paste-mismatch (e.g. {@code h1} vs
     *  {@code h1:27017}, or {@code H1} vs {@code h1}) doesn't sneak
     *  the primary past the client-side guard.</p> */
    public static boolean wouldTargetPrimary(String targetHost,
                                             String primaryHost) {
        return normalize(targetHost).equals(normalize(primaryHost));
    }

    static String normalize(String hostPort) {
        if (hostPort == null) return "";
        String s = hostPort.trim().toLowerCase();
        // Default port if omitted — rs.conf stores the explicit port
        // but user-pasted strings often drop it.
        if (!s.contains(":")) s = s + ":27017";
        return s;
    }
}
