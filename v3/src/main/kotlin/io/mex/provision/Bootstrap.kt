package io.mex.provision

import io.mex.data.Lab
import io.mex.data.LabTopology
import java.security.SecureRandom

/*
 * Pure mongosh scripts for the bootstrap phases (PRV-BOOT-3/4/5) plus the registered-URI
 * builder (PRV-CONN-1). Scripts print sentinels the runner asserts on — exit codes alone
 * don't distinguish "primary elected" from "mongosh connected and did nothing".
 */

const val ROOT_USER = "root"

private val PASSWORD_ALPHABET = ('A'..'Z') + ('a'..'z') + ('0'..'9')

/** Alphanumeric only, so the password never needs URI-encoding (PRV-SEC-3). */
fun generatePassword(length: Int = 24): String {
    val rnd = SecureRandom()
    return (1..length).map { PASSWORD_ALPHABET[rnd.nextInt(PASSWORD_ALPHABET.size)] }.joinToString("")
}

/**
 * Initiates a replica set with an explicit member list and waits for a primary.
 * Idempotent: NotYetInitialized is the only state that triggers rs.initiate, so a
 * resumed bootstrap replays safely (same guard as the `testing/` rig scripts).
 */
fun initiateScript(rsName: String, members: List<String>, configSvr: Boolean): String {
    val memberList = members.mapIndexed { i, svc -> "{_id: $i, host: \"$svc:27017\"}" }.joinToString(", ")
    val cfgField = if (configSvr) "configsvr: true, " else ""
    return """
        try { rs.status(); print('ALREADY'); } catch (e) {
          if (e.codeName === 'NotYetInitialized') {
            rs.initiate({_id: '$rsName', ${cfgField}members: [$memberList]});
          } else { throw e; }
        }
        let elected = false;
        for (let i = 0; i < 120 && !elected; i++) {
          try {
            elected = rs.status().members.some(m => m.stateStr === 'PRIMARY');
          } catch (e) { /* transient during election */ }
          if (!elected) sleep(1000);
        }
        if (elected) { print('PRIMARY'); } else { print('NO_PRIMARY'); quit(1); }
    """.trimIndent()
}

/** `sh.addShard` through a mongos; server-side idempotent, so replays are safe (PRV-BOOT-4). */
fun addShardScript(rsName: String, members: List<String>): String {
    val seed = "$rsName/" + members.joinToString(",") { "$it:27017" }
    return """
        const r = sh.addShard('$seed');
        if (r.ok === 1) { print('ADDED'); } else { print('ADD_FAILED ' + JSON.stringify(r)); quit(1); }
    """.trimIndent()
}

/**
 * Creates the root user via the localhost exception (PRV-BOOT-5). Prints NOT_PRIMARY on
 * a secondary so the runner can walk the member list until it lands on the primary.
 */
fun createRootScript(password: String): String = """
    if (!db.hello().isWritablePrimary) { print('NOT_PRIMARY'); quit(2); }
    db.getSiblingDB('admin').createUser({user: '$ROOT_USER', pwd: '$password', roles: [{role: 'root', db: 'admin'}]});
    print('CREATED');
""".trimIndent()

/* ===================== registered connection URI (PRV-CONN-1) ===================== */

/**
 * Builds the URI the connection store registers. RS labs use directConnection to the
 * first member — members advertise compose service names the host cannot resolve, so
 * driver-side topology discovery would dead-end (PRV-CONN-2). Sharded labs list every
 * mongos; standalone is a plain single-host URI.
 */
fun labUri(lab: Lab, portMap: Map<String, Int>, password: String?): String {
    val ports = plan(lab).clientServices.mapNotNull { portMap[it] }
    require(ports.isNotEmpty()) { "no client ports allocated" }
    val cred = if (lab.auth && password != null) "$ROOT_USER:$password@" else ""
    val params = buildList {
        if (lab.topology is LabTopology.ReplicaSet) add("directConnection=true")
        if (lab.auth && password != null) add("authSource=admin")
    }
    val hosts = when (lab.topology) {
        is LabTopology.Sharded -> ports.joinToString(",") { "127.0.0.1:$it" }
        else -> "127.0.0.1:${ports.first()}"
    }
    val query = if (params.isEmpty()) "" else "?" + params.joinToString("&")
    return "mongodb://$cred$hosts/$query"
}

/* ===================== log redaction (PRV-SEC-3, PRV-NFR-4) ===================== */

/** Masks every secret occurrence before a line reaches the log buffer or the screen. */
fun redact(line: String, secrets: Collection<String>): String =
    secrets.filter { it.isNotEmpty() }.fold(line) { acc, s -> acc.replace(s, "•••") }
