package io.mex.mongo

import com.mongodb.client.MongoClient
import org.bson.Document

/**
 * One row of `$currentOp`. [opidRaw] keeps the server's own value untouched — a plain
 * number on a replica set but a `"shard01:12345"` string through mongos — because
 * `killOp` must receive it back exactly as issued.
 */
data class CurrentOp(
    val opidRaw: Any?,
    val opid: String,
    val active: Boolean,
    val op: String,
    val ns: String,
    val secsRunning: Long?,
    val planSummary: String?,
    val appName: String?,
    val client: String?,
    val effectiveUser: String?,
    val waitingForLock: Boolean,
    val command: String?,
    val desc: String?,
) {
    /** A running query with no index to use — the classic "why is the cluster slow". */
    val collscan: Boolean get() = planSummary == "COLLSCAN"

    /** Server-internal ops that are never worth killing and mostly noise. */
    val system: Boolean
        get() = desc?.let { d ->
            d.startsWith("Checkpointer") || d.startsWith("JournalFlusher") ||
                d.startsWith("WT") || d.startsWith("Oplog") || d.startsWith("rsSync") ||
                d.startsWith("Repl") || d.startsWith("Noop") || d.startsWith("monitoring") ||
                d.startsWith("TTLMonitor") || d.startsWith("ClusterTime")
        } ?: false
}

/**
 * Snapshot of in-flight operations via the `$currentOp` aggregation stage (DBA-OP-1).
 * `allUsers` needs the `inprog` privilege; without it the server returns only the
 * caller's own ops, which is still useful — errors propagate to the panel.
 */
fun listCurrentOps(client: MongoClient, includeIdle: Boolean = false): List<CurrentOp> {
    val stage = Document(
        "\$currentOp",
        Document("allUsers", true)
            .append("idleConnections", includeIdle)
            .append("idleCursors", false),
    )
    return client.getDatabase("admin")
        .aggregate(listOf(stage))
        .map { it.toCurrentOp() }
        .into(mutableListOf())
}

private fun Document.toCurrentOp(): CurrentOp {
    val opidRaw = this["opid"]
    val users = (this["effectiveUsers"] as? List<*>)
        ?.filterIsInstance<Document>()
        ?.joinToString(",") { it.getString("user") ?: "?" }
        ?.ifBlank { null }
    val cmd = (this["command"] as? Document)?.toJson()
    return CurrentOp(
        opidRaw = opidRaw,
        opid = opidRaw?.toString() ?: "—",
        active = this["active"] == true,
        op = this["op"]?.toString() ?: "none",
        ns = this["ns"]?.toString().orEmpty(),
        secsRunning = (this["secs_running"] as? Number)?.toLong(),
        planSummary = this["planSummary"]?.toString(),
        appName = this["appName"]?.toString()?.ifBlank { null },
        client = (this["client"] ?: this["client_s"])?.toString(),
        effectiveUser = users,
        waitingForLock = this["waitingForLock"] == true,
        command = cmd,
        desc = this["desc"]?.toString(),
    )
}

/**
 * Terminates one in-flight operation (DBA-OP-2). The opid goes back verbatim —
 * stringifying a numeric opid (or vice versa) makes mongos reject the kill.
 */
fun killOp(client: MongoClient, opidRaw: Any?) {
    requireNotNull(opidRaw) { "Operation has no opid" }
    client.getDatabase("admin").runCommand(Document("killOp", 1).append("op", opidRaw))
}
