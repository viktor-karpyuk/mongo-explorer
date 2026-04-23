package com.kubrik.mex.maint.sharded;

import com.kubrik.mex.maint.index.RollingIndexPlanner;
import com.kubrik.mex.maint.index.RollingIndexRunner;
import com.kubrik.mex.maint.model.IndexBuildSpec;
import com.kubrik.mex.maint.model.ReconfigSpec;
import com.kubrik.mex.maint.model.ReconfigSpec.Member;
import com.kubrik.mex.maint.reconfig.ReconfigSerializer;
import com.mongodb.client.MongoClient;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 IDX-BLD-* — 3-node rolling index build against the sharded rig
 * ({@code testing/db-sharded} with {@code MEX_SHARDED_RIG=up}).
 *
 * <p>Uses the modern post-Q2.7-C1 runner: one {@code createIndexes}
 * with {@code commitQuorum: "votingMembers"} via the primary lets the
 * server orchestrate the rolling build; the runner then verifies each
 * member has the index by listing indexes directly.</p>
 */
@Tag("shardedRig")
@EnabledIfEnvironmentVariable(named = "MEX_SHARDED_RIG", matches = "up")
class RollingIndexShardedIT {

    private static MongoClient mongos;
    private static MongoClient seed;
    private static final String DB = "app";
    private static final String COLL = "items";
    private static final String IDX_NAME = "n_rolling_it";

    @BeforeAll
    static void setUp() {
        // mongos for writes — always routes to the current primary,
        // avoids the "seed got demoted" flake if the last election
        // didn't pick shard1a.
        mongos = ShardedRigSupport.openMongos();
        seed = ShardedRigSupport.openMember(ShardedRigSupport.SHARD1A_PORT);
        com.mongodb.client.MongoDatabase db = mongos.getDatabase(DB);
        db.getCollection(COLL).drop();
        List<Document> docs = new java.util.ArrayList<>();
        for (int i = 0; i < 100; i++) {
            docs.add(new Document("n", i).append("name", "item-" + i));
        }
        db.getCollection(COLL).insertMany(docs);
    }

    @AfterAll
    static void tearDown() {
        if (mongos != null) {
            try {
                mongos.getDatabase(DB).getCollection(COLL).dropIndex(IDX_NAME);
            } catch (Exception ignored) {}
            mongos.close();
        }
        if (seed != null) seed.close();
    }

    @Test
    void rolling_build_lands_on_every_member() {
        // 1. Read the rs config to get the member list.
        Document reply = seed.getDatabase("admin").runCommand(
                new Document("replSetGetConfig", 1));
        List<Member> members = parseMembers(reply);
        int primaryId = findPrimaryId(seed);
        assertTrue(primaryId >= 0);

        // 2. Plan — secondaries first, primary last (for UI display).
        List<RollingIndexPlanner.Step> plan = new RollingIndexPlanner()
                .plan(members, primaryId);
        assertEquals(3, plan.size());
        assertTrue(plan.get(plan.size() - 1).isPrimary());

        String primaryHost = plan.get(plan.size() - 1).member().host();
        List<String> memberHosts = plan.stream()
                .map(s -> s.member().host()).toList();

        IndexBuildSpec spec = new IndexBuildSpec(DB, COLL,
                new Document("n", 1), IDX_NAME, false, false,
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());

        RollingIndexRunner.DispatchContext ctx = host -> {
            int port;
            if (host.startsWith("shard1a")) port = ShardedRigSupport.SHARD1A_PORT;
            else if (host.startsWith("shard1b")) port = ShardedRigSupport.SHARD1B_PORT;
            else if (host.startsWith("shard1c")) port = ShardedRigSupport.SHARD1C_PORT;
            else throw new IllegalArgumentException(host);
            return ShardedRigSupport.openMember(port);
        };

        // 3. Modern runner: single createIndexes via the primary
        //    with commitQuorum=votingMembers + per-member verify.
        RollingIndexRunner runner = new RollingIndexRunner();
        RollingIndexRunner.Result result = runner.run(
                ctx, spec, primaryHost, memberHosts);
        assertTrue(result.overallSuccess(),
                "per-member outcomes: " + result.perMember());
        assertEquals(3, result.perMember().size());

        // 4. Independent verification — listIndexes directly via each
        //    member's host-published port (doesn't go through the
        //    DispatchContext used by the runner).
        for (RollingIndexPlanner.Step step : plan) {
            int port = ctxPort(step.member().host());
            try (MongoClient member = ShardedRigSupport.openMember(port)) {
                boolean present = false;
                for (Document ix : member.getDatabase(DB)
                        .getCollection(COLL).listIndexes()) {
                    if (IDX_NAME.equals(ix.getString("name"))) {
                        present = true; break;
                    }
                }
                assertTrue(present,
                        IDX_NAME + " missing on " + step.member().host());
            }
        }
    }

    private static int ctxPort(String host) {
        if (host.startsWith("shard1a")) return ShardedRigSupport.SHARD1A_PORT;
        if (host.startsWith("shard1b")) return ShardedRigSupport.SHARD1B_PORT;
        if (host.startsWith("shard1c")) return ShardedRigSupport.SHARD1C_PORT;
        throw new IllegalArgumentException(host);
    }

    // Reuse the serializer so the IT picks up the Int32/Int64/Double
    // coercion fix. A locally-rewritten parseMembers hit a ClassCast
    // on live rs.conf because `priority` comes back as Int32 on
    // server-created defaults but Long / Double on older or ALTERed
    // fields.
    private static List<Member> parseMembers(Document cfgReply) {
        return new ReconfigSerializer().fromConfigReply(cfgReply,
                new ReconfigSpec.ChangePriority(0, 1))
                .map(ReconfigSpec.Request::currentMembers)
                .orElseThrow();
    }

    @SuppressWarnings("unchecked")
    private static int findPrimaryId(MongoClient client) {
        Document status = client.getDatabase("admin").runCommand(
                new Document("replSetGetStatus", 1));
        for (Document m : (List<Document>) status.get("members", List.class)) {
            if ("PRIMARY".equals(m.getString("stateStr"))) {
                return m.getInteger("_id", -1);
            }
        }
        return -1;
    }
}
