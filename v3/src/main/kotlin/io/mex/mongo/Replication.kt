package io.mex.mongo

import com.mongodb.client.MongoClient
import org.bson.BsonTimestamp
import org.bson.Document

/* ============================ models ============================ */

/**
 * Shape of the oplog (DBA-OPLOG-1). The window — newest minus oldest entry — is the
 * time a secondary can stay down and still catch up by replaying the oplog instead of
 * needing a full resync; it is the number that decides whether an outage is urgent.
 */
data class OplogInfo(
    val firstTsSec: Long,
    val lastTsSec: Long,
    val usedBytes: Long,
    val maxBytes: Long,
) {
    val windowSeconds: Long get() = (lastTsSec - firstTsSec).coerceAtLeast(0)
    val usedPercent: Double get() = if (maxBytes > 0) usedBytes * 100.0 / maxBytes else 0.0
}

enum class ReplSeverity { ok, warn, critical }

/** < 1 h of oplog margin is an emergency; < 24 h deserves attention. */
fun oplogWindowSeverity(windowSeconds: Long): ReplSeverity = when {
    windowSeconds >= 86_400 -> ReplSeverity.ok
    windowSeconds >= 3_600 -> ReplSeverity.warn
    else -> ReplSeverity.critical
}

/** Sustained lag ≥ 60 s is critical; ≥ 10 s is worth watching. */
fun lagSeverity(lagSeconds: Long): ReplSeverity = when {
    lagSeconds < 10 -> ReplSeverity.ok
    lagSeconds < 60 -> ReplSeverity.warn
    else -> ReplSeverity.critical
}

data class MemberOptime(val name: String, val state: String, val optimeMillis: Long?)

/**
 * Per-secondary lag relative to the primary's optime (DBA-OPLOG-2).
 *
 * Lag must be primary.optime − member.optime: comparing a member's optime against the
 * local wall clock (as the first cut of the Cluster panel did) folds clock skew and
 * app-machine latency into the number and is meaningless on an idle cluster.
 */
fun lagAgainstPrimary(members: List<MemberOptime>): Map<String, Long> {
    val primary = members.firstOrNull { it.state == "PRIMARY" }?.optimeMillis ?: return emptyMap()
    return members
        .filter { it.state == "SECONDARY" && it.optimeMillis != null }
        .associate { it.name to ((primary - it.optimeMillis!!) / 1000).coerceAtLeast(0) }
}

/* ============================ queries ============================ */

/** Reads the oplog's shape from `local.oplog.rs` — needs read on `local`. */
fun fetchOplogInfo(client: MongoClient): OplogInfo {
    val local = client.getDatabase("local")
    val oplog = local.getCollection("oplog.rs")
    fun boundary(direction: Int): Long? {
        val doc = oplog.find()
            .sort(Document("\$natural", direction))
            .projection(Document("ts", 1))
            .limit(1)
            .firstOrNull() ?: return null
        return (doc["ts"] as? BsonTimestamp)?.time?.toLong()
    }
    val first = boundary(1) ?: 0L
    val last = boundary(-1) ?: 0L
    val stats = local.runCommand(Document("collStats", "oplog.rs"))
    return OplogInfo(
        firstTsSec = first,
        lastTsSec = last,
        usedBytes = (stats["size"] as? Number)?.toLong() ?: 0L,
        maxBytes = (stats["maxSize"] as? Number)?.toLong() ?: 0L,
    )
}

/** Member optimes from `replSetGetStatus`, for primary-relative lag. */
fun fetchMemberOptimes(client: MongoClient): List<MemberOptime> {
    val status = client.getDatabase("admin").runCommand(Document("replSetGetStatus", 1))
    return (status["members"] as? List<*>).orEmpty().filterIsInstance<Document>().map { m ->
        val stateCode = (m["state"] as? Number)?.toInt() ?: 6
        MemberOptime(
            name = m.getString("name") ?: "?",
            state = when (stateCode) {
                1 -> "PRIMARY"
                2 -> "SECONDARY"
                7 -> "ARBITER"
                else -> "OTHER"
            },
            optimeMillis = (m["optimeDate"] as? java.util.Date)?.time,
        )
    }
}
