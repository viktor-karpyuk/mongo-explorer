package io.mex.mongo

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import org.bson.Document
import java.util.concurrent.TimeUnit

/** `sh.startBalancer()` / `sh.stopBalancer()` — reversible, but pausing stops chunk migrations. */
fun setBalancer(client: MongoClient, enabled: Boolean) {
    client.getDatabase("admin").runCommand(Document(if (enabled) "balancerStart" else "balancerStop", 1))
}

/* ============================ shard removal ============================ */

data class RemoveShardStatus(
    val state: String,            // started | ongoing | completed (server vocabulary)
    val remainingChunks: Long?,
    val remainingDbs: Long?,
    /** Databases whose primary lives on the draining shard — they need movePrimary. */
    val dbsToMove: List<String>,
)

internal fun parseRemoveShardResult(res: Document): RemoveShardStatus {
    val remaining = res["remaining"] as? Document
    return RemoveShardStatus(
        state = res.getString("state") ?: "unknown",
        remainingChunks = (remaining?.get("chunks") as? Number)?.toLong(),
        remainingDbs = (remaining?.get("dbs") as? Number)?.toLong(),
        dbsToMove = (res["dbsToMove"] as? List<*>).orEmpty().filterIsInstance<String>(),
    )
}

/**
 * Starts draining [shard], or reports progress if the drain is already running — the
 * server makes the same command mean both, which is why the UI's typed confirm only
 * guards the not-yet-draining case.
 */
fun removeShard(client: MongoClient, shard: String): RemoveShardStatus =
    parseRemoveShardResult(client.getDatabase("admin").runCommand(Document("removeShard", shard)))

/**
 * `rs.freeze(seconds)` — keeps a secondary from seeking election for [seconds]; 0 lifts
 * an existing freeze. The command only works on the member itself, and our client talks
 * to the set as a whole, so a short-lived direct client carries it there.
 */
fun freezeMember(baseUri: String, host: String, seconds: Int) {
    val settings = MongoClientSettings.builder()
        .applicationName("mongo-explorer-v3")
        .applyConnectionString(ConnectionString(directNodeUri(baseUri, host)))
        .applyToClusterSettings { it.serverSelectionTimeout(8, TimeUnit.SECONDS) }
        .applyToSocketSettings { it.connectTimeout(8, TimeUnit.SECONDS) }
        .build()
    MongoClients.create(settings).use { c ->
        c.getDatabase("admin").runCommand(Document("replSetFreeze", seconds))
    }
}
