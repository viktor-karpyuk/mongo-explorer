package com.kubrik.mex.maint.reconfig;

import com.kubrik.mex.maint.model.ReconfigSpec;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

/**
 * v2.7 Q2.7-D — Dispatches a {@code replSetReconfig} command against
 * a live cluster and surfaces the outcome. Orchestration-only; the
 * quorum math lives in {@link ReconfigPreflight}, the BSON shape in
 * {@link ReconfigSerializer}, and the post-change wait in
 * {@link PostChangeVerifier}.
 *
 * <p>The runner is not an FX component and takes no locks. Callers
 * (a wizard, a CLI, a test) build the {@link ReconfigSpec.Request},
 * run the preflight, require an APPROVED approval row, and only then
 * hand the request here.</p>
 *
 * <p>A 60 s watchdog per RCFG-5 bounds the dispatch — the driver's
 * built-in timeout can stretch further for a slow-election cluster,
 * which is exactly the condition the wizard wants to surface as
 * "still running" in the UI.</p>
 */
public final class ReconfigRunner {

    private static final Logger log = LoggerFactory.getLogger(ReconfigRunner.class);

    /** RCFG-5 — 60 s hard watchdog. */
    public static final Duration DEFAULT_WATCHDOG = Duration.ofSeconds(60);

    private final ReconfigSerializer serializer;
    private final Duration watchdog;

    public ReconfigRunner() {
        this(new ReconfigSerializer(), DEFAULT_WATCHDOG);
    }

    public ReconfigRunner(ReconfigSerializer serializer, Duration watchdog) {
        this.serializer = serializer;
        this.watchdog = watchdog;
    }

    public sealed interface Outcome permits Outcome.Ok, Outcome.Failed, Outcome.TimedOut {
        record Ok(int newConfigVersion, Document driverReply) implements Outcome {}
        record Failed(String code, String message) implements Outcome {}
        record TimedOut(Duration elapsed) implements Outcome {}
    }

    /** Dispatch the reconfig. Caller guarantees the preflight has
     *  passed and an APPROVED approval row exists. Returns the
     *  outcome; does NOT mark the approval consumed — that's the
     *  caller's job so the CONSUMED transition is atomic with the
     *  audit row it gates. */
    public Outcome dispatch(MongoClient client, ReconfigSpec.Request req) {
        Document body = serializer.toReconfigBody(req);
        Document cmd = new Document("replSetReconfig", body)
                // force=false — explicit because server defaults changed
                // across 4.4→5.0 and we want the same behaviour on both.
                .append("force", false)
                .append("maxTimeMS", watchdog.toMillis())
                // writeConcern majority per RCFG-5.
                .append("writeConcern", new Document("w", "majority"));

        long t0 = System.currentTimeMillis();
        try {
            MongoDatabase admin = client.getDatabase("admin");
            Document reply = admin.runCommand(cmd);
            double ok = reply.get("ok") instanceof Number n ? n.doubleValue() : 0.0;
            if (ok >= 1.0) {
                return new Outcome.Ok(serializer.bumpedVersion(
                        req.currentConfigVersion()), reply);
            }
            String code = reply.getString("codeName");
            String msg = reply.getString("errmsg");
            log.warn("replSetReconfig returned ok={} code={} msg={}", ok, code, msg);
            return new Outcome.Failed(code == null ? "UNKNOWN" : code,
                    msg == null ? "no error message" : msg);
        } catch (com.mongodb.MongoExecutionTimeoutException te) {
            return new Outcome.TimedOut(
                    Duration.ofMillis(System.currentTimeMillis() - t0));
        } catch (com.mongodb.MongoException me) {
            log.warn("replSetReconfig driver exception: {}", me.getMessage());
            return new Outcome.Failed(
                    me.getCode() == 0 ? "DRIVER" : String.valueOf(me.getCode()),
                    me.getMessage());
        }
    }

    /** Build the rollback plan JSON for the given {@code currentConfig}
     *  reply — the caller stores this alongside the audit row. Keeping
     *  the builder here keeps the "apply + rollback are paired" pattern
     *  obvious to a reader. */
    public String buildRollbackPlanJson(Document currentConfigReply) {
        Optional<ReconfigSpec.Request> typed = serializer.fromConfigReply(
                currentConfigReply, new ReconfigSpec.WholeConfig(
                        java.util.List.of()));  // dummy change
        if (typed.isEmpty()) {
            // Raw passthrough — at least the JSON is recoverable.
            return currentConfigReply == null ? "{}"
                    : currentConfigReply.toJson();
        }
        // Emit a plan the replay runner can feed straight back into
        // replSetReconfig: the original config body, unchanged.
        Document priorBody = new Document()
                .append("command", "replSetReconfig")
                .append("priorConfig", currentConfigReply.get("config"));
        return priorBody.toJson();
    }
}
