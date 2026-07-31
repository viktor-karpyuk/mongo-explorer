package io.mex.mongo

import com.mongodb.client.MongoClient
import org.bson.Document

/* ============================ server log ============================ */

/** One structured log line (MongoDB ≥ 4.4 logs JSON per line). */
data class LogLine(
    val ts: String,
    val severity: String, // F E W I D1..D5
    val component: String,
    val context: String,
    val message: String,
    val attrJson: String?,
    val raw: String,
)

/** Higher means more severe; used by the panel's minimum-severity filter. */
fun severityRank(s: String): Int = when (s.firstOrNull()) {
    'F' -> 4
    'E' -> 3
    'W' -> 2
    'I' -> 1
    else -> 0 // D*, unknown
}

/**
 * Parses one line of `getLog` output. Structured lines are JSON; anything that fails to
 * parse (older servers, truncated lines) is kept verbatim rather than dropped.
 */
fun parseLogLine(line: String): LogLine {
    val doc = runCatching { Document.parse(line) }.getOrNull()
        ?: return LogLine("", "I", "", "", line, null, line)
    // Document.parse turns {"$date": …} into a java.util.Date whose toString is
    // locale-formatted noise — render it back as an ISO-8601 instant.
    val ts = when (val t = doc["t"]) {
        is java.util.Date -> java.time.Instant.ofEpochMilli(t.time).toString()
        is Document -> t["\$date"]?.toString().orEmpty()
        else -> t?.toString().orEmpty()
    }
    return LogLine(
        ts = ts,
        severity = doc["s"]?.toString() ?: "I",
        component = doc["c"]?.toString().orEmpty(),
        context = doc["ctx"]?.toString().orEmpty(),
        message = doc["msg"]?.toString() ?: line,
        attrJson = (doc["attr"] as? Document)?.toJson(),
        raw = line,
    )
}

/** `getLog: "global"` — the server's recent in-memory log tail. */
fun fetchServerLog(client: MongoClient): List<LogLine> {
    val res = client.getDatabase("admin").runCommand(Document("getLog", "global"))
    return (res["log"] as? List<*>).orEmpty().map { parseLogLine(it.toString()) }
}

/**
 * `getLog: "startupWarnings"` — the warnings people discover months late (THP enabled,
 * missing ulimits, no access control, XFS recommendation…).
 */
fun fetchStartupWarnings(client: MongoClient): List<LogLine> {
    val res = client.getDatabase("admin").runCommand(Document("getLog", "startupWarnings"))
    return (res["log"] as? List<*>).orEmpty().map { parseLogLine(it.toString()) }
}

/* ============================ parameters ============================ */

data class ParamRow(
    val name: String,
    val value: String,
    /** Known default for the curated tunables; null when we don't know the default. */
    val default: String?,
    val tuned: Boolean,
)

/**
 * Defaults for the parameters DBAs actually tune. Deliberately curated — pretending to
 * know the default of every one of ~600 parameters across versions would be dishonest;
 * anything not listed simply shows without a verdict.
 */
val KNOWN_PARAM_DEFAULTS: Map<String, Any> = mapOf(
    "journalCommitInterval" to 100,
    "syncdelay" to 60.0,
    "ttlMonitorEnabled" to true,
    "ttlMonitorSleepSecs" to 60,
    "cursorTimeoutMillis" to 600_000,
    "transactionLifetimeLimitSeconds" to 60,
    "maxTransactionLockRequestTimeoutMillis" to 5,
    "notablescan" to false,
    "quiet" to false,
    "logLevel" to 0,
    "maxIndexBuildMemoryUsageMegabytes" to 200,
    "internalQueryMaxBlockingSortMemoryUsageBytes" to 104_857_600,
    "enableFlowControl" to true,
    "flowControlTargetLagSeconds" to 10.0,
    "diagnosticDataCollectionEnabled" to true,
    "allowDiskUseByDefault" to true,
)

/** Numeric-tolerant comparison — BSON returns Int/Long/Double interchangeably. */
internal fun sameValue(a: Any?, b: Any?): Boolean = when {
    a is Number && b is Number -> a.toDouble() == b.toDouble()
    a is Boolean && b is Boolean -> a == b
    else -> a?.toString() == b?.toString()
}

/**
 * Turns a `getParameter: '*'` result into display rows, flagging curated tunables that
 * differ from their default (the DriftDiffEngine idea, scoped honestly).
 */
fun classifyParameters(params: Map<String, Any?>, defaults: Map<String, Any> = KNOWN_PARAM_DEFAULTS): List<ParamRow> =
    params.entries
        .filter { it.key != "ok" && !it.key.startsWith("$") && it.key != "operationTime" }
        .map { (name, value) ->
            val default = defaults[name]
            ParamRow(
                name = name,
                value = value?.toString() ?: "null",
                default = default?.toString(),
                tuned = default != null && !sameValue(value, default),
            )
        }
        .sortedWith(compareByDescending<ParamRow> { it.tuned }.thenBy { it.name })

fun fetchServerParameters(client: MongoClient): List<ParamRow> {
    val res = client.getDatabase("admin").runCommand(Document("getParameter", "*"))
    return classifyParameters(res)
}
