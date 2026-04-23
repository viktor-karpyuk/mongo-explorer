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
 * v2.7 IDX-BLD-4/5/6 — Dispatches {@code createIndexes} across a
 * replica set using MongoDB's built-in 2-phase commit protocol.
 *
 * <p><b>Modern pattern (6.0+):</b> fire {@code createIndexes} once
 * via the primary with {@code commitQuorum: "votingMembers"}. The
 * server drives the rolling build itself — each secondary clones
 * the in-progress build, the command returns only after the majority
 * has committed. This replaces the pre-4.4 "stop each member and
 * build as standalone" pattern that the first draft encoded; that
 * pattern tried to send {@code createIndexes} with
 * {@code commitQuorum: 0} directly to each secondary, which 6.0+
 * rejects with {@code NotWritablePrimary}.</p>
 *
 * <p>Per-member visibility is preserved by post-build verification:
 * after the primary command returns, {@link #run} opens a direct
 * connection to every member (via {@link DispatchContext}) and asks
 * {@code listIndexes} whether the named index is present. The UI
 * uses the returned {@link MemberOutcome} list for the per-member
 * progress strip.</p>
 *
 * <p>Abort is a single {@code dropIndexes} via the primary; the
 * server's same 2-phase machinery cleans up the in-progress build
 * across every member.</p>
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

    /** Fire the build once via the primary with
     *  {@code commitQuorum: "votingMembers"}; the server drives the
     *  rolling protocol internally. After the command returns (which
     *  is once a majority has committed), verify per-member via
     *  {@code listIndexes}.
     *
     *  @param ctx           supplies member-level clients for the
     *                       per-member verification pass
     *  @param spec          what to build
     *  @param primaryHost   {@code host:port} of the current primary
     *  @param memberHosts   every data-bearing member (including the
     *                       primary) in the replset; the verifier
     *                       checks each one
     */
    public Result run(DispatchContext ctx, IndexBuildSpec spec,
                      String primaryHost, List<String> memberHosts) {
        long t0 = System.currentTimeMillis();
        // 1. Dispatch via the primary. commitQuorum="votingMembers"
        //    asks the server to wait until every voting member has
        //    committed before returning — gives us majority durability
        //    without tying up the UI on each secondary individually.
        MongoClient primary = null;
        try {
            primary = ctx.openMember(primaryHost);
            MongoDatabase db = primary.getDatabase(spec.db());
            Document cmd = new Document("createIndexes", spec.coll())
                    .append("indexes", spec.asCreateIndexesArgs())
                    .append("commitQuorum", "votingMembers");
            db.runCommand(cmd);
        } catch (com.mongodb.MongoCommandException mce) {
            if (primary != null) primary.close();
            // Fire-time failure — nothing committed on any member.
            return new Result(
                    List.of(new MemberOutcome(-1, primaryHost, false,
                            mce.getErrorCodeName(), mce.getErrorMessage(),
                            System.currentTimeMillis() - t0)),
                    false, System.currentTimeMillis() - t0);
        } catch (Exception e) {
            if (primary != null) primary.close();
            return new Result(
                    List.of(new MemberOutcome(-1, primaryHost, false,
                            e.getClass().getSimpleName(), e.getMessage(),
                            System.currentTimeMillis() - t0)),
                    false, System.currentTimeMillis() - t0);
        } finally {
            if (primary != null) {
                try { primary.close(); } catch (Exception ignored) {}
            }
        }

        // 2. Per-member verification — listIndexes on every node and
        //    check the named index is present. A lagging secondary
        //    that hasn't replicated the commit shows up as failure;
        //    the UI surfaces it distinctly from a hard build failure.
        List<MemberOutcome> outcomes = new ArrayList<>(memberHosts.size());
        for (String host : memberHosts) {
            outcomes.add(verifyIndexOn(ctx, spec, host));
        }
        boolean overall = outcomes.stream().allMatch(MemberOutcome::success);
        return new Result(List.copyOf(outcomes), overall,
                System.currentTimeMillis() - t0);
    }

    private MemberOutcome verifyIndexOn(DispatchContext ctx,
                                        IndexBuildSpec spec, String host) {
        long t0 = System.currentTimeMillis();
        try (MongoClient member = ctx.openMember(host)) {
            MongoDatabase db = member.getDatabase(spec.db());
            for (Document ix : db.getCollection(spec.coll()).listIndexes()) {
                if (spec.name().equals(ix.getString("name"))) {
                    return new MemberOutcome(-1, host, true, null,
                            "index present",
                            System.currentTimeMillis() - t0);
                }
            }
            return new MemberOutcome(-1, host, false, "IndexNotFound",
                    "index " + spec.name() + " not yet on " + host
                            + " — likely lagging",
                    System.currentTimeMillis() - t0);
        } catch (Exception e) {
            return new MemberOutcome(-1, host, false,
                    e.getClass().getSimpleName(), e.getMessage(),
                    System.currentTimeMillis() - t0);
        }
    }

    /** IDX-BLD-6 — abort / rollback. Modern pattern: a single
     *  {@code dropIndexes} via the primary propagates across every
     *  member through the same 2-phase machinery that ran the build.
     *  No per-member killOp dance. */
    public Result abort(DispatchContext ctx, IndexBuildSpec spec,
                        String primaryHost) {
        long t0 = System.currentTimeMillis();
        try (MongoClient primary = ctx.openMember(primaryHost)) {
            MongoDatabase db = primary.getDatabase(spec.db());
            db.runCommand(new Document("dropIndexes", spec.coll())
                    .append("index", spec.name()));
            return new Result(
                    List.of(new MemberOutcome(-1, primaryHost, true, null,
                            "dropIndexes " + spec.name()
                                    + " dispatched — replicating",
                            System.currentTimeMillis() - t0)),
                    true, System.currentTimeMillis() - t0);
        } catch (com.mongodb.MongoCommandException mce) {
            // IndexNotFound here means either the build failed before
            // landing anywhere (benign) or a second abort is running
            // over the same index. Either way the cluster state is
            // "no such index" which is the intent; treat as success.
            if ("IndexNotFound".equals(mce.getErrorCodeName())) {
                return new Result(
                        List.of(new MemberOutcome(-1, primaryHost, true,
                                null, "index already absent",
                                System.currentTimeMillis() - t0)),
                        true, System.currentTimeMillis() - t0);
            }
            return new Result(
                    List.of(new MemberOutcome(-1, primaryHost, false,
                            mce.getErrorCodeName(), mce.getErrorMessage(),
                            System.currentTimeMillis() - t0)),
                    false, System.currentTimeMillis() - t0);
        } catch (Exception e) {
            return new Result(
                    List.of(new MemberOutcome(-1, primaryHost, false,
                            e.getClass().getSimpleName(), e.getMessage(),
                            System.currentTimeMillis() - t0)),
                    false, System.currentTimeMillis() - t0);
        }
    }

    /** Indirection so the caller controls how member-level clients are
     *  opened. Production uses the existing ConnectionManager's
     *  peer-client cache; tests pass fakes. */
    @FunctionalInterface
    public interface DispatchContext {
        MongoClient openMember(String hostPort);
    }
}
