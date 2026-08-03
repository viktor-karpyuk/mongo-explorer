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

/** One shard as reported by listShards; hosts come from its "rs/h1,h2" host string. */
data class ShardInfo(
    val name: String,
    val rsName: String?,
    val hosts: List<String>,
    val draining: Boolean,
)

data class RouterInfo(
    val host: String,
    val lastPingAgeSecs: Long?,
    val version: String?,
) {
    // config.mongos keeps every router that ever joined; only recently-pinging
    // ones are alive. 60s would flap on a paused lab, 1h hides real departures.
    val active: Boolean get() = lastPingAgeSecs != null && lastPingAgeSecs <= 300
}

data class ShardedTopology(
    val routers: List<RouterInfo>,
    val configRsName: String?,
    val configHosts: List<String>,
    val shards: List<ShardInfo>,
    val balancerEnabled: Boolean?,
)

data class ClusterSnapshot(
    val topology: TopologyInfo,
    val health: HealthReport,
    val rsConfig: RsConfig?,
    val sharded: ShardedTopology? = null,
    /** The endpoint this client is talking to — the only node we can name on a standalone. */
    val endpoint: String? = null,
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

    var sharded: ShardedTopology? = null
    if (hello["msg"] == "isdbgrid") {
        type = "sharded"
        sharded = shardedTopology(client)
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

    val endpoint = (hello["me"] as? String)
        ?: runCatching {
            client.clusterDescription.serverDescriptions.firstOrNull()?.address?.toString()
        }.getOrNull()

    val topology = TopologyInfo(type, setName, primary, members)
    return ClusterSnapshot(topology, scoreHealth(topology), rsConfig, sharded, endpoint)
}

/**
 * Everything here degrades independently: a user allowed to run listShards may
 * still be denied config.mongos or balancerStatus, and a partial diagram beats none.
 */
private fun shardedTopology(client: MongoClient): ShardedTopology {
    val admin = client.getDatabase("admin")

    val shards = runCatching {
        val res = admin.runCommand(Document("listShards", 1))
        (res["shards"] as? List<*>).orEmpty().filterIsInstance<Document>().map { s ->
            val (rs, hosts) = parseHostString(s.getString("host") ?: "")
            ShardInfo(
                name = s.getString("_id") ?: "?",
                rsName = rs,
                hosts = hosts,
                draining = s["draining"] == true,
            )
        }
    }.getOrElse { emptyList() }

    val routers = runCatching {
        val now = System.currentTimeMillis()
        client.getDatabase("config").getCollection("mongos").find().limit(64).map { d ->
            RouterInfo(
                host = d.getString("_id") ?: "?",
                lastPingAgeSecs = (d["ping"] as? java.util.Date)
                    ?.let { ((now - it.time) / 1000).coerceAtLeast(0) },
                version = d.getString("mongoVersion"),
            )
        }.toList()
    }.getOrElse { emptyList() }

    val (cfgRs, cfgHosts) = runCatching {
        val ss = admin.runCommand(Document("serverStatus", 1))
        val cs = (ss["sharding"] as? Document)?.get("configsvrConnectionString") as? String
        parseHostString(cs ?: "")
    }.getOrElse { null to emptyList() }

    val balancer = runCatching {
        admin.runCommand(Document("balancerStatus", 1)).getString("mode") == "full"
    }.getOrNull()

    return ShardedTopology(routers, cfgRs, cfgHosts, shards, balancer)
}

/** `"rs0/h1:27017,h2:27017"` → `("rs0", [h1:27017, h2:27017])`; without `/` the set name is null. */
internal fun parseHostString(hostString: String): Pair<String?, List<String>> {
    if (hostString.isBlank()) return null to emptyList()
    val slash = hostString.indexOf('/')
    val rs = if (slash >= 0) hostString.take(slash) else null
    val hosts = hostString.substring(slash + 1).split(',').map { it.trim() }.filter { it.isNotEmpty() }
    return rs to hosts
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
