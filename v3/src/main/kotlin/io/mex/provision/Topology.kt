package io.mex.provision

import io.mex.data.LabTopology

/** Curated `mongo:<tag>` images offered by the wizard (PRV-TOPO-4). First entry is the default. */
val MONGO_TAGS = listOf("8.0", "7.0", "6.0")

/**
 * Per-process resident estimates for the footprint line (PRV-TOPO-5). mongod is capped at a
 * 0.25 GB WiredTiger cache by the renderer, so ~350 MiB covers cache + overhead; mongos is
 * stateless and lighter.
 */
private const val MONGOD_EST_MIB = 350
private const val MONGOS_EST_MIB = 150

/** Above this estimate the wizard shows a non-blocking warning (PRV-TOPO-5). */
const val FOOTPRINT_WARN_MIB = 8 * 1024

data class Footprint(val containers: Int, val estMemMiB: Int, val volumes: Int)

private val RS_MEMBER_COUNTS = setOf(1, 3, 5, 7)

/** PRV-TOPO-3 — pure and total; an empty result means the topology is buildable. */
fun validate(t: LabTopology): List<String> = buildList {
    when (t) {
        is LabTopology.Standalone -> Unit
        is LabTopology.ReplicaSet ->
            if (t.members !in RS_MEMBER_COUNTS) add("Replica set members must be 1, 3, 5 or 7.")
        is LabTopology.Sharded -> {
            if (t.shards !in 1..6) add("Shards must be between 1 and 6.")
            if (t.membersPerShard != 1 && t.membersPerShard != 3) add("Members per shard must be 1 or 3.")
            if (t.mongos !in 1..3) add("Mongos routers must be between 1 and 3.")
            if (t.configServers != 1 && t.configServers != 3) add("Config servers must be 1 or 3.")
        }
    }
}

/** Only mongods get data volumes — mongos is stateless. */
fun footprint(t: LabTopology): Footprint = when (t) {
    is LabTopology.Standalone -> Footprint(1, MONGOD_EST_MIB, 1)
    is LabTopology.ReplicaSet -> Footprint(t.members, t.members * MONGOD_EST_MIB, t.members)
    is LabTopology.Sharded -> {
        val mongods = t.shards * t.membersPerShard + t.configServers
        Footprint(
            containers = mongods + t.mongos,
            estMemMiB = mongods * MONGOD_EST_MIB + t.mongos * MONGOS_EST_MIB,
            volumes = mongods,
        )
    }
}

/** One-line topology description for lab rows and the sidebar (`sharded 3×3 · csrs 3 · mongos 2`). */
fun summary(t: LabTopology): String = when (t) {
    is LabTopology.Standalone -> "standalone"
    is LabTopology.ReplicaSet -> "replica set ×${t.members}"
    is LabTopology.Sharded ->
        "sharded ${t.shards}×${t.membersPerShard} · csrs ${t.configServers} · mongos ${t.mongos}"
}
