package io.mex.mongo

import com.mongodb.client.MongoClient
import org.bson.Document

/* ============================ step down ============================ */

/** The exact command a step-down preview shows and the runner sends (DBA-RCFG-1). */
fun stepDownCommand(stepDownSecs: Int, catchUpSecs: Int): Document =
    Document("replSetStepDown", stepDownSecs)
        .append("secondaryCatchUpPeriodSecs", catchUpSecs)

/**
 * Asks the primary to yield. The driver routes admin commands to the primary, which is
 * exactly the member that must receive this. On 4.0-era servers the primary closes all
 * connections on step-down, so a network error here can still mean success.
 */
fun stepDownPrimary(client: MongoClient, stepDownSecs: Int, catchUpSecs: Int) {
    client.getDatabase("admin").runCommand(stepDownCommand(stepDownSecs, catchUpSecs))
}

/* ============================ member reconfig ============================ */

/** The editable slice of one member's configuration. */
data class MemberPatch(
    val id: Int,
    val host: String,
    val priority: Double,
    val votes: Int,
    val hidden: Boolean,
    val secondaryDelaySecs: Long,
)

/**
 * Server-enforced consistency rules, checked client-side so the dialog can explain the
 * problem instead of relaying an opaque reconfig error (DBA-RCFG-2).
 */
fun validateMemberPatch(patch: MemberPatch): List<String> = buildList {
    if (patch.priority < 0 || patch.priority > 1000) add("Priority must be between 0 and 1000.")
    if (patch.votes !in 0..1) add("Votes must be 0 or 1.")
    if (patch.hidden && patch.priority > 0) add("A hidden member must have priority 0.")
    if (patch.secondaryDelaySecs > 0 && patch.priority > 0) add("A delayed member must have priority 0.")
    if (patch.secondaryDelaySecs < 0) add("Delay cannot be negative.")
    if (patch.priority > 0 && patch.votes == 0) add("A member with priority > 0 must have a vote.")
}

/**
 * Applies [patch] onto a fresh raw `replSetGetConfig` document, bumping the version.
 * Only the four editable fields change; everything else round-trips untouched.
 */
fun buildReconfig(rawConfig: Document, patch: MemberPatch): Document {
    val next = Document(rawConfig)
    next["version"] = ((rawConfig["version"] as? Number)?.toInt() ?: 1) + 1
    val members = (rawConfig["members"] as? List<*>).orEmpty().filterIsInstance<Document>().map { m ->
        if ((m["_id"] as? Number)?.toInt() != patch.id) return@map Document(m)
        Document(m).apply {
            put("priority", patch.priority)
            put("votes", patch.votes)
            put("hidden", patch.hidden)
            // 5.0 renamed slaveDelay; write the modern field and drop the legacy one.
            remove("slaveDelay")
            put("secondaryDelaySecs", patch.secondaryDelaySecs)
        }
    }
    next["members"] = members
    return next
}

/** Human-readable change list for the preview step (pure, so the diff is testable). */
fun reconfigDiff(before: RsMemberConfig, after: MemberPatch): List<String> = buildList {
    if (before.priority != after.priority) add("priority ${formatNum(before.priority)} → ${formatNum(after.priority)}")
    if (before.votes != after.votes) add("votes ${before.votes} → ${after.votes}")
    if (before.hidden != after.hidden) add(if (after.hidden) "becomes hidden" else "no longer hidden")
    if (before.secondaryDelaySecs != after.secondaryDelaySecs) {
        add("delay ${before.secondaryDelaySecs}s → ${after.secondaryDelaySecs}s")
    }
}

private fun formatNum(v: Double): String = if (v == v.toLong().toDouble()) "${v.toLong()}" else "$v"

/**
 * How the voting majority moves if [patch] lands — the number that decides whether the
 * set can still elect a primary. Shown in the preview whenever it changes.
 */
fun majorityShift(members: List<RsMemberConfig>, patch: MemberPatch): Pair<Int, Int> {
    val before = members.count { it.votes > 0 }
    val after = members.count { if (it.id == patch.id) patch.votes > 0 else it.votes > 0 }
    return (before / 2 + 1) to (after / 2 + 1)
}

fun fetchRawRsConfig(client: MongoClient): Document {
    val res = client.getDatabase("admin").runCommand(Document("replSetGetConfig", 1))
    return (res["config"] as? Document) ?: res
}

fun applyReconfig(client: MongoClient, newConfig: Document) {
    client.getDatabase("admin").runCommand(Document("replSetReconfig", newConfig))
}
