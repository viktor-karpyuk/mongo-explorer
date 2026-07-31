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
    val priority: Double,
    val votes: Int,
)

data class TopologyInfo(
    val type: String,        // standalone | replicaset | sharded | unknown
    val setName: String?,
    val primary: String?,
    val members: List<MemberInfo>,
)

data class HealthReport(val score: Int, val reasons: List<String>)

/** One member as declared in rs.conf (config-side view, independent of runtime state). */
data class RsMemberConfig(
    val id: Int,
    val host: String,
    val priority: Double,
    val votes: Int,
    val arbiterOnly: Boolean,
    val hidden: Boolean,
    val buildIndexes: Boolean,
    val secondaryDelaySecs: Long,
    val tags: Map<String, String>,
)

/** A single rs.conf settings entry, pre-rendered for display. */
data class RsSetting(
    val label: String,
    val value: String,
    val isDefault: Boolean,
)

/** Structured replSetGetConfig result; [json] keeps the pretty EJSON for copy/raw view. */
data class RsConfig(
    val setName: String,
    val version: Int,
    val term: Long?,
    val protocolVersion: Long?,
    val configServer: Boolean,
    val members: List<RsMemberConfig>,
    val settings: List<RsSetting>,
    val replicaSetId: String?,
    val json: String,
) {
    val votingMembers: Int get() = members.count { it.votes > 0 }
    val majority: Int get() = votingMembers / 2 + 1
    val arbiters: Int get() = members.count { it.arbiterOnly }
    val hiddenMembers: Int get() = members.count { it.hidden }
    val delayedMembers: Int get() = members.count { it.secondaryDelaySecs > 0 }
}

data class ClusterSnapshot(
    val topology: TopologyInfo,
    val health: HealthReport,
    val rsConfig: RsConfig?,
)

private val STATE_BY_CODE = mapOf(
    0 to "STARTUP", 1 to "PRIMARY", 2 to "SECONDARY", 3 to "RECOVERING",
    5 to "STARTUP2", 6 to "UNKNOWN", 7 to "ARBITER", 8 to "DOWN",
    9 to "ROLLBACK", 10 to "REMOVED",
)

private val PRETTY_EJSON: JsonWriterSettings =
    JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).indent(true).build()

fun clusterSnapshot(client: MongoClient): ClusterSnapshot {
    val admin = client.getDatabase("admin")
    val hello = admin.runCommand(Document("hello", 1))

    var type = "unknown"
    var setName: String? = null
    var primary: String? = null
    val members = mutableListOf<MemberInfo>()
    var rsConfig: RsConfig? = null

    if (hello["msg"] == "isdbgrid") {
        type = "sharded"
    } else if (hello["setName"] is String) {
        type = "replicaset"
        setName = hello.getString("setName")
        primary = hello["primary"] as? String
        rsConfig = runCatching {
            val cfg = admin.runCommand(Document("replSetGetConfig", 1))
            parseRsConfig((cfg["config"] as? Document) ?: cfg)
        }.getOrNull()
        runCatching {
            val status = admin.runCommand(Document("replSetGetStatus", 1))
            val byHost = rsConfig?.members?.associateBy { it.host }.orEmpty()
            val sm = (status["members"] as? List<*>).orEmpty().filterIsInstance<Document>()
            // Lag is measured against the primary's optime, not the local wall clock —
            // clock skew made the old now-based number nonsense on idle clusters.
            val primaryOptime = sm.firstOrNull { (it["state"] as? Number)?.toInt() == 1 }
                ?.let { it["optimeDate"] as? java.util.Date }?.time
            for (m in sm) {
                val name = m.getString("name")
                val stateCode = (m["state"] as? Number)?.toInt() ?: 6
                val optime = (m["optimeDate"] as? java.util.Date)?.time
                val lag = when {
                    stateCode == 1 -> 0L
                    stateCode == 2 && optime != null && primaryOptime != null ->
                        ((primaryOptime - optime) / 1000).coerceAtLeast(0)
                    else -> null
                }
                members += MemberInfo(
                    name = name,
                    state = STATE_BY_CODE[stateCode] ?: "UNKNOWN",
                    health = ((m["health"] as? Number)?.toInt() ?: 0),
                    uptime = (m["uptime"] as? Number)?.toLong() ?: 0L,
                    pingMs = (m["pingMs"] as? Number)?.toLong(),
                    lagSeconds = lag,
                    priority = byHost[name]?.priority ?: 1.0,
                    votes = byHost[name]?.votes ?: 1,
                )
            }
        }
    } else {
        type = "standalone"
    }

    val topology = TopologyInfo(type, setName, primary, members)
    return ClusterSnapshot(topology, scoreHealth(topology), rsConfig)
}

private fun parseRsConfig(cfg: Document): RsConfig {
    val members = (cfg["members"] as? List<*>).orEmpty().filterIsInstance<Document>().map { m ->
        RsMemberConfig(
            id = (m["_id"] as? Number)?.toInt() ?: -1,
            host = m.getString("host") ?: "?",
            priority = (m["priority"] as? Number)?.toDouble() ?: 1.0,
            votes = (m["votes"] as? Number)?.toInt() ?: 1,
            arbiterOnly = m["arbiterOnly"] == true,
            hidden = m["hidden"] == true,
            buildIndexes = m["buildIndexes"] != false,
            // secondaryDelaySecs since 5.0; slaveDelay before.
            secondaryDelaySecs = ((m["secondaryDelaySecs"] ?: m["slaveDelay"]) as? Number)?.toLong() ?: 0L,
            tags = (m["tags"] as? Document)?.entries?.associate { it.key to it.value.toString() }.orEmpty(),
        )
    }

    val s = cfg["settings"] as? Document
    val settings = buildList {
        fun add(label: String, raw: Any?, default: Any?, render: (Any) -> String = Any::toString) {
            val effective = raw ?: default ?: return
            add(RsSetting(label, render(effective), raw == null || numEq(raw, default)))
        }
        add("Chaining allowed", s?.get("chainingAllowed"), true)
        add("Heartbeat interval", s?.get("heartbeatIntervalMillis"), 2_000) { renderMillis(it) }
        add("Heartbeat timeout", s?.get("heartbeatTimeoutSecs"), 10) { "$it s" }
        add("Election timeout", s?.get("electionTimeoutMillis"), 10_000) { renderMillis(it) }
        add("Catch-up timeout", s?.get("catchUpTimeoutMillis"), -1) {
            if ((it as Number).toLong() == -1L) "unlimited" else renderMillis(it)
        }
        add("Catch-up takeover delay", s?.get("catchUpTakeoverDelayMillis"), 30_000) { renderMillis(it) }
        add("Majority journal default", cfg["writeConcernMajorityJournalDefault"], true)
        val modes = s?.get("getLastErrorModes") as? Document
        if (modes != null && modes.isNotEmpty()) {
            add(RsSetting("Custom write-concern modes", modes.keys.joinToString(", "), false))
        }
        val gleDefaults = s?.get("getLastErrorDefaults") as? Document
        if (gleDefaults != null && !(gleDefaults.size == 2 && numEq(gleDefaults["w"], 1) && numEq(gleDefaults["wtimeout"], 0))) {
            add(RsSetting("getLastError defaults", gleDefaults.toJson(), false))
        }
    }

    return RsConfig(
        setName = cfg.getString("_id") ?: "?",
        version = (cfg["version"] as? Number)?.toInt() ?: 0,
        term = (cfg["term"] as? Number)?.toLong(),
        protocolVersion = (cfg["protocolVersion"] as? Number)?.toLong(),
        configServer = cfg["configsvr"] == true,
        members = members,
        settings = settings,
        replicaSetId = (s?.get("replicaSetId"))?.toString(),
        json = cfg.toJson(PRETTY_EJSON),
    )
}

/** Numeric-tolerant equality: BSON hands back Integer/Long/Double interchangeably. */
private fun numEq(a: Any?, b: Any?): Boolean = when {
    a is Number && b is Number -> a.toDouble() == b.toDouble()
    else -> a == b
}

private fun renderMillis(v: Any): String {
    val ms = (v as Number).toLong()
    return if (ms % 1000 == 0L) "${ms / 1000} s" else "$ms ms"
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
