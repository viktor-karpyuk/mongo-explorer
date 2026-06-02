package io.mex.mongo

import com.mongodb.client.MongoClient
import org.bson.Document

data class MonitorTick(
    val ts: Long,
    val opsInsert: Long, val opsQuery: Long, val opsUpdate: Long,
    val opsDelete: Long, val opsGetMore: Long, val opsCommand: Long,
    val networkInBytes: Long, val networkOutBytes: Long,
    val currentConnections: Long, val availableConnections: Long,
    val residentMb: Long,
    val wtCacheBytesUsed: Long, val wtCacheBytesMax: Long,
    val latencyReadsAvgUs: Double?, val latencyWritesAvgUs: Double?,
) {
    val opsTotal: Long get() = opsInsert + opsQuery + opsUpdate + opsDelete + opsGetMore + opsCommand
    val wtCachePercent: Double get() = if (wtCacheBytesMax == 0L) 0.0 else wtCacheBytesUsed.toDouble() / wtCacheBytesMax * 100.0
}

fun tick(client: MongoClient): MonitorTick {
    val s = client.getDatabase("admin").runCommand(Document("serverStatus", 1))
    val ops = (s["opcounters"] as? Document) ?: Document()
    val network = (s["network"] as? Document) ?: Document()
    val conns = (s["connections"] as? Document) ?: Document()
    val mem = (s["mem"] as? Document) ?: Document()
    val wt = ((s["wiredTiger"] as? Document)?.get("cache") as? Document) ?: Document()
    val ol = (s["opLatencies"] as? Document) ?: Document()

    fun n(doc: Document, key: String) = (doc[key] as? Number)?.toLong() ?: 0L
    fun latencyAvg(name: String): Double? {
        val sub = ol[name] as? Document ?: return null
        val opsCount = (sub["ops"] as? Number)?.toLong() ?: 0L
        val latency = (sub["latency"] as? Number)?.toLong() ?: 0L
        return if (opsCount > 0) latency.toDouble() / opsCount else null
    }

    return MonitorTick(
        ts = System.currentTimeMillis(),
        opsInsert = n(ops, "insert"),
        opsQuery = n(ops, "query"),
        opsUpdate = n(ops, "update"),
        opsDelete = n(ops, "delete"),
        opsGetMore = n(ops, "getmore"),
        opsCommand = n(ops, "command"),
        networkInBytes = n(network, "bytesIn"),
        networkOutBytes = n(network, "bytesOut"),
        currentConnections = n(conns, "current"),
        availableConnections = n(conns, "available"),
        residentMb = n(mem, "resident"),
        wtCacheBytesUsed = (wt["bytes currently in the cache"] as? Number)?.toLong() ?: 0L,
        wtCacheBytesMax = (wt["maximum bytes configured"] as? Number)?.toLong() ?: 0L,
        latencyReadsAvgUs = latencyAvg("reads"),
        latencyWritesAvgUs = latencyAvg("writes"),
    )
}

data class ProfilerLevel(val level: Int, val slowMs: Int)

data class SlowOp(val op: String, val ns: String, val millis: Long, val ts: Long, val command: String?)

fun getProfilerLevel(client: MongoClient, db: String): ProfilerLevel {
    val r = client.getDatabase(db).runCommand(Document("profile", -1))
    return ProfilerLevel(
        level = (r["was"] as? Number)?.toInt() ?: 0,
        slowMs = (r["slowms"] as? Number)?.toInt() ?: 100,
    )
}

fun setProfilerLevel(client: MongoClient, db: String, p: ProfilerLevel) {
    client.getDatabase(db).runCommand(Document("profile", p.level).append("slowms", p.slowMs))
}

fun listSlowOps(client: MongoClient, db: String, limit: Int = 50): List<SlowOp> = runCatching {
    client.getDatabase(db).getCollection("system.profile")
        .find().sort(Document("ts", -1)).limit(limit)
        .into(mutableListOf<Document>())
        .map {
            SlowOp(
                op = it.getString("op") ?: "",
                ns = it.getString("ns") ?: "",
                millis = (it["millis"] as? Number)?.toLong() ?: 0L,
                ts = (it["ts"] as? java.util.Date)?.time ?: 0L,
                command = (it["command"] as? Document)?.toJson(),
            )
        }
}.getOrDefault(emptyList())
