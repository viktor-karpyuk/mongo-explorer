package io.mex.mongo

import com.mongodb.client.MongoClient
import org.bson.Document
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings

data class MemberInfo(
    val name: String,
    val state: String,
    val health: Int,
    val uptime: Long,
    val pingMs: Long?,
    val lagSeconds: Long?,
    val priority: Int,
    val votes: Int,
)

data class TopologyInfo(
    val type: String,        // standalone | replicaset | sharded | unknown
    val setName: String?,
    val primary: String?,
    val members: List<MemberInfo>,
)

data class HealthReport(val score: Int, val reasons: List<String>)

data class ClusterSnapshot(
    val topology: TopologyInfo,
    val health: HealthReport,
    val rsConfigJson: String?,
)

private val STATE_BY_CODE = mapOf(
    0 to "STARTUP", 1 to "PRIMARY", 2 to "SECONDARY", 3 to "RECOVERING",
    5 to "STARTUP2", 6 to "UNKNOWN", 7 to "ARBITER", 8 to "DOWN",
    9 to "ROLLBACK", 10 to "REMOVED",
)

private val EJSON: JsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build()

fun clusterSnapshot(client: MongoClient): ClusterSnapshot {
    val admin = client.getDatabase("admin")
    val hello = admin.runCommand(Document("hello", 1))

    var type = "unknown"
    var setName: String? = null
    var primary: String? = null
    val members = mutableListOf<MemberInfo>()
    var rsConfigJson: String? = null

    if (hello["msg"] == "isdbgrid") {
        type = "sharded"
    } else if (hello["setName"] is String) {
        type = "replicaset"
        setName = hello.getString("setName")
        primary = hello["primary"] as? String
        runCatching {
            val status = admin.runCommand(Document("replSetGetStatus", 1))
            val priorities = runCatching {
                val cfg = admin.runCommand(Document("replSetGetConfig", 1))
                val cfgMembers = ((cfg["config"] as? Document)?.get("members") as? List<*>).orEmpty()
                cfgMembers.filterIsInstance<Document>().associate {
                    it.getString("host") to ((it["priority"] as? Number)?.toInt() to (it["votes"] as? Number)?.toInt())
                }
            }.getOrDefault(emptyMap())
            val sm = (status["members"] as? List<*>).orEmpty().filterIsInstance<Document>()
            for (m in sm) {
                val name = m.getString("name")
                val stateCode = (m["state"] as? Number)?.toInt() ?: 6
                val optime = m["optimeDate"] as? java.util.Date
                members += MemberInfo(
                    name = name,
                    state = STATE_BY_CODE[stateCode] ?: "UNKNOWN",
                    health = ((m["health"] as? Number)?.toInt() ?: 0),
                    uptime = (m["uptime"] as? Number)?.toLong() ?: 0L,
                    pingMs = (m["pingMs"] as? Number)?.toLong(),
                    lagSeconds = optime?.let { ((System.currentTimeMillis() - it.time) / 1000).coerceAtLeast(0) },
                    priority = priorities[name]?.first ?: 1,
                    votes = priorities[name]?.second ?: 1,
                )
            }
        }
        rsConfigJson = runCatching {
            val cfg = admin.runCommand(Document("replSetGetConfig", 1))
            ((cfg["config"] as? Document) ?: cfg).toJson(EJSON)
        }.getOrNull()
    } else {
        type = "standalone"
    }

    val topology = TopologyInfo(type, setName, primary, members)
    return ClusterSnapshot(topology, scoreHealth(topology), rsConfigJson)
}

private fun scoreHealth(t: TopologyInfo): HealthReport {
    val reasons = mutableListOf<String>()
    var score = 100
    if (t.type == "standalone") return HealthReport(score, listOf("Standalone — no replication metrics."))
    if (t.type == "sharded") return HealthReport(score, listOf("Sharded cluster — score per shard."))
    if (t.members.isEmpty()) return HealthReport(50, listOf("Could not read replSetGetStatus (auth?)."))
    val hasPrimary = t.members.any { it.state == "PRIMARY" }
    if (!hasPrimary) { score -= 50; reasons += "No primary elected" }
    for (m in t.members) {
        if (m.health != 1 || m.state == "DOWN") { score -= 30; reasons += "${m.name} is ${m.state}" }
        if (m.lagSeconds != null && m.lagSeconds > 10 && m.state == "SECONDARY") {
            score -= 15; reasons += "${m.name} lag ${m.lagSeconds}s"
        }
    }
    return HealthReport(score.coerceIn(0, 100), reasons)
}
