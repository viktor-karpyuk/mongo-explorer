package com.kubrik.mex.maint.sharded;

import com.kubrik.mex.maint.model.ReconfigSpec;
import com.kubrik.mex.maint.reconfig.PostChangeVerifier;
import com.kubrik.mex.maint.reconfig.ReconfigPreflight;
import com.kubrik.mex.maint.reconfig.ReconfigRunner;
import com.kubrik.mex.maint.reconfig.ReconfigSerializer;
import com.mongodb.client.MongoClient;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 RCFG-* — 3-node sharded-rig coverage for the reconfig pipeline.
 * Uses the sample rig at {@code testing/db-sharded} (bring it up with
 * {@code docker compose -f testing/db-sharded/docker-compose.yml up -d}
 * and export {@code MEX_SHARDED_RIG=up}).
 *
 * <p>Exercises priority-bump + vote-flip round-trips against the real
 * shard1rs replica set — things the single-node {@link
 * com.kubrik.mex.maint.reconfig.ReconfigPreflightIT} can't reach.</p>
 */
@Tag("shardedRig")
@EnabledIfEnvironmentVariable(named = "MEX_SHARDED_RIG", matches = "up")
class ReconfigWizardShardedIT {

    private static MongoClient shardPrimary;
    private static final ReconfigSerializer SER = new ReconfigSerializer();
    private static final ReconfigPreflight PRE = new ReconfigPreflight();
    private static final ReconfigRunner RUN = new ReconfigRunner();

    @BeforeAll
    static void open() {
        // Connect via shard1a (direct). If it's a secondary the driver
        // will reject writes; we use it only for replSetGetConfig +
        // replSetReconfig which are admin commands.
        shardPrimary = ShardedRigSupport.openMember(ShardedRigSupport.SHARD1A_PORT);
    }

    @AfterAll
    static void close() {
        if (shardPrimary != null) shardPrimary.close();
    }

    @Test
    void priority_bump_round_trips_through_the_full_pipeline() {
        // 1. Fetch current rs.conf.
        Document reply = shardPrimary.getDatabase("admin").runCommand(
                new Document("replSetGetConfig", 1));
        Optional<ReconfigSpec.Request> parsed = SER.fromConfigReply(
                reply, new ReconfigSpec.ChangePriority(0, 2));
        assertTrue(parsed.isPresent());
        ReconfigSpec.Request req = parsed.get();
        assertEquals("shard1rs", req.replicaSetName());
        assertEquals(3, req.currentMembers().size());

        // 2. Preflight clean.
        ReconfigPreflight.Result r = PRE.check(req);
        assertFalse(r.hasBlocking());

        // 3. Dispatch via a client pointing at the current primary —
        //    ask the set whose primary owns writes.
        Document status = shardPrimary.getDatabase("admin").runCommand(
                new Document("replSetGetStatus", 1));
        String primaryHost = findPrimaryHost(status);
        assertNotNull(primaryHost, "sh rs must have a primary");

        // Use a fresh client to the primary for the reconfig.
        int expectedNewVersion = SER.bumpedVersion(req.currentConfigVersion());
        try (MongoClient primaryClient = openByHost(primaryHost)) {
            ReconfigRunner.Outcome outcome = RUN.dispatch(primaryClient, req);
            assertInstanceOf(ReconfigRunner.Outcome.Ok.class, outcome);

            // 4. Verify majority catch-up within 120s.
            PostChangeVerifier verifier = new PostChangeVerifier();
            PostChangeVerifier.Verdict v = verifier.awaitConvergence(
                    primaryClient, expectedNewVersion);
            assertTrue(v.converged(),
                    "majority should catch up; lagging=" + v.lagging());
        }

        // 5. Undo — bump priority back to 1 so repeat runs start clean.
        Document latest = shardPrimary.getDatabase("admin").runCommand(
                new Document("replSetGetConfig", 1));
        try (MongoClient primaryClient = openByHost(primaryHost)) {
            RUN.dispatch(primaryClient, SER.fromConfigReply(latest,
                    new ReconfigSpec.ChangePriority(0, 1)).orElseThrow());
        }
    }

    @SuppressWarnings("unchecked")
    private static String findPrimaryHost(Document status) {
        for (Document m : (java.util.List<Document>) status.get("members",
                java.util.List.class)) {
            if ("PRIMARY".equals(m.getString("stateStr"))) {
                return m.getString("name");
            }
        }
        return null;
    }

    private static MongoClient openByHost(String hostPort) {
        // rs.status reports the container-network name (e.g.
        // shard1a:27017). Map that back to the host-published port
        // so the IT (running on the host) can reach it.
        int port;
        if (hostPort.startsWith("shard1a")) port = ShardedRigSupport.SHARD1A_PORT;
        else if (hostPort.startsWith("shard1b")) port = ShardedRigSupport.SHARD1B_PORT;
        else if (hostPort.startsWith("shard1c")) port = ShardedRigSupport.SHARD1C_PORT;
        else throw new IllegalArgumentException("unknown member " + hostPort);
        return ShardedRigSupport.openMember(port);
    }
}
