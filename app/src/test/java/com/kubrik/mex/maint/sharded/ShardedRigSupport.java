package com.kubrik.mex.maint.sharded;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/**
 * v2.7 — Helper that gates ITs behind the external sharded rig at
 * {@code testing/db-sharded}. The rig is not a testcontainers
 * fixture; it's brought up out of band ({@code docker compose up -d})
 * so CI can decide whether to run the sharded suite based on
 * environment rather than per-class container lifecycle.
 *
 * <p>Usage in a test class:</p>
 * <pre>
 * &#64;Tag("shardedRig")
 * &#64;EnabledIfEnvironmentVariable(named = "MEX_SHARDED_RIG", matches = "up")
 * class RollingIndexShardedIT { ... }
 * </pre>
 *
 * <p>When {@code MEX_SHARDED_RIG=up} is set, tests open clients
 * against the fixed ports published by the compose file:
 * mongos on {@value MONGOS_PORT}, cfgrs members on
 * {@value CFG1_PORT}/{@value CFG2_PORT}/{@value CFG3_PORT}, and
 * shard1rs members on {@value SHARD1A_PORT}/{@value SHARD1B_PORT}/
 * {@value SHARD1C_PORT}.</p>
 */
public final class ShardedRigSupport {

    public static final int MONGOS_PORT = 27100;
    public static final int CFG1_PORT = 27101;
    public static final int CFG2_PORT = 27102;
    public static final int CFG3_PORT = 27103;
    public static final int SHARD1A_PORT = 27111;
    public static final int SHARD1B_PORT = 27112;
    public static final int SHARD1C_PORT = 27113;

    /** Direct client to a specific member (bypasses mongos). */
    public static MongoClient openMember(int port) {
        return MongoClients.create(
                "mongodb://localhost:" + port + "/?directConnection=true");
    }

    /** Cluster client via mongos — for sh.status, addShard, etc. */
    public static MongoClient openMongos() {
        return MongoClients.create("mongodb://localhost:" + MONGOS_PORT);
    }

    private ShardedRigSupport() {}
}
