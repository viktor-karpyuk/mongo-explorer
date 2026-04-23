package com.kubrik.mex.maint.index;

import com.kubrik.mex.maint.model.IndexBuildSpec;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * v2.7 IDX-BLD-4/5/6 — Dispatches {@code createIndexes} against
 * each member in the plan produced by {@link RollingIndexPlanner}.
 *
 * <p>Key detail: {@code commitQuorum: 0} turns the 2-phase commit
 * OFF so the build is node-local, which is what "rolling" means.
 * Builds on members in the planner order; per-member outcome lands
 * in {@link Result} so the audit row shows exactly which node failed
 * (if any).</p>
 *
 * <p>The step-down for the primary is handled as a separate
 * {@code replSetStepDown} command before the per-member dispatch —
 * keeps the dispatch uniform and makes the step-down inspectable in
 * an audit row.</p>
 */
public final class RollingIndexRunner {

    private static final Logger log = LoggerFactory.getLogger(RollingIndexRunner.class);

    public record MemberOutcome(
            int memberId, String host, boolean success,
            String errorCode, String errorMessage, long elapsedMs) {}

    public record Result(
            List<MemberOutcome> perMember,
            boolean overallSuccess,
            long totalElapsedMs) {}

    /** Dispatch the full rolling build. Caller supplies a
     *  {@link DispatchContext} wrapping per-member MongoClient access;
     *  keeps this class decoupled from how the UI opens peer clients. */
    public Result run(DispatchContext ctx, IndexBuildSpec spec,
                      List<RollingIndexPlanner.Step> steps) {
        long t0 = System.currentTimeMillis();
        List<MemberOutcome> outcomes = new ArrayList<>(steps.size());
        for (RollingIndexPlanner.Step step : steps) {
            MemberOutcome outcome = runOne(ctx, spec, step);
            outcomes.add(outcome);
            if (!outcome.success()) {
                log.warn("rolling index build failed on {} — bailing. {} members completed.",
                        outcome.host(), outcomes.size() - 1);
                break;
            }
        }
        boolean overall = outcomes.stream().allMatch(MemberOutcome::success);
        return new Result(List.copyOf(outcomes), overall,
                System.currentTimeMillis() - t0);
    }

    private MemberOutcome runOne(DispatchContext ctx, IndexBuildSpec spec,
                                  RollingIndexPlanner.Step step) {
        long t0 = System.currentTimeMillis();
        try (MongoClient member = ctx.openMember(step.member().host())) {
            if (step.isPrimary()) {
                // Step down first so the build runs on a secondary,
                // then we return to this member for the build.
                MongoDatabase admin = member.getDatabase("admin");
                admin.runCommand(new Document("replSetStepDown", 60)
                        .append("force", false));
            }
            MongoDatabase db = member.getDatabase(spec.db());
            Document cmd = new Document("createIndexes", spec.coll())
                    .append("indexes", spec.asCreateIndexesArgs())
                    // 0 = build on this member only; no 2-phase commit.
                    .append("commitQuorum", 0);
            db.runCommand(cmd);
            return new MemberOutcome(step.member().id(), step.member().host(),
                    true, null, null, System.currentTimeMillis() - t0);
        } catch (Exception e) {
            return new MemberOutcome(step.member().id(), step.member().host(),
                    false, e.getClass().getSimpleName(), e.getMessage(),
                    System.currentTimeMillis() - t0);
        }
    }

    /** IDX-BLD-6 — abort path. Drops the index on members that
     *  completed, and {@code killOp}s the currently-building op if
     *  supplied. Best-effort; every individual step is captured in
     *  the outcome list so the audit row documents the state. */
    public Result abort(DispatchContext ctx, IndexBuildSpec spec,
                        List<MemberOutcome> completed, Long activeOpId,
                        String activeHost) {
        long t0 = System.currentTimeMillis();
        List<MemberOutcome> drops = new ArrayList<>();
        for (MemberOutcome m : completed) {
            if (!m.success()) continue;
            long ts = System.currentTimeMillis();
            try (MongoClient member = ctx.openMember(m.host())) {
                MongoDatabase db = member.getDatabase(spec.db());
                db.runCommand(new Document("dropIndexes", spec.coll())
                        .append("index", spec.name()));
                drops.add(new MemberOutcome(m.memberId(), m.host(),
                        true, null, null, System.currentTimeMillis() - ts));
            } catch (Exception e) {
                drops.add(new MemberOutcome(m.memberId(), m.host(),
                        false, e.getClass().getSimpleName(), e.getMessage(),
                        System.currentTimeMillis() - ts));
            }
        }
        if (activeOpId != null && activeHost != null) {
            long ts = System.currentTimeMillis();
            try (MongoClient active = ctx.openMember(activeHost)) {
                MongoDatabase admin = active.getDatabase("admin");
                admin.runCommand(new Document("killOp", 1)
                        .append("op", activeOpId));
                drops.add(new MemberOutcome(-1, activeHost, true,
                        null, "killOp " + activeOpId + " sent",
                        System.currentTimeMillis() - ts));
            } catch (com.mongodb.MongoSocketException expected) {
                // Server tore down the connection on killOp — expected
                // when the operation was actively running.
                drops.add(new MemberOutcome(-1, activeHost, true,
                        null, "killOp " + activeOpId
                                + " (connection closed)",
                        System.currentTimeMillis() - ts));
            } catch (Exception e) {
                log.warn("killOp failed on {}: {}", activeHost, e.getMessage());
                drops.add(new MemberOutcome(-1, activeHost, false,
                        e.getClass().getSimpleName(), e.getMessage(),
                        System.currentTimeMillis() - ts));
            }
        }
        boolean overall = drops.stream().allMatch(MemberOutcome::success);
        return new Result(List.copyOf(drops), overall,
                System.currentTimeMillis() - t0);
    }

    /** Indirection so the caller controls how member-level clients are
     *  opened. Production uses the existing ConnectionManager's
     *  peer-client cache; tests pass fakes. */
    @FunctionalInterface
    public interface DispatchContext {
        MongoClient openMember(String hostPort);
    }
}
