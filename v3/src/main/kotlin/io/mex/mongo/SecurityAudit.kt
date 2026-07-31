package io.mex.mongo

import com.mongodb.client.MongoClient
import org.bson.Document

/* ============================ models ============================ */

enum class CheckLevel { pass, warn, fail, info }

/** One CIS-style security check outcome (DBA-CIS-1). */
data class SecurityCheck(
    val id: String,
    val title: String,
    val level: CheckLevel,
    val detail: String,
    /** What to do about it — empty for passing/info checks. */
    val remediation: String = "",
)

/** Raw facts gathered read-only, so the rule logic below stays pure and testable. */
data class SecurityFacts(
    val authEnabled: Boolean,
    val tlsMode: String?,        // disabled | allowTLS | preferTLS | requireTLS | null (unknown)
    val bindIp: String?,
    val bindIpAll: Boolean,
    val serverVersion: String,
    val userCount: Int,
    val superuserCount: Int,
    val usersWithoutTls: Boolean, // SCRAM users fine; the check is about x509/none
    val javascriptEnabled: Boolean?,
    val auditLogEnabled: Boolean?,
    val clusterAuthMode: String?,
)

/* ===================== pure rule logic (tested) ===================== */

/** Latest widely-deployed GA majors at time of writing, for the EOL/CVE heuristic. */
private const val LATEST_MAJOR = 8

fun parseMajor(version: String): Int? =
    version.substringBefore('.').toIntOrNull()

/**
 * Turns gathered facts into the checklist. Deliberately covers the handful of settings that
 * are both high-impact and cheaply observable read-only — not a full CIS benchmark, which
 * would need host-level access this tool doesn't have.
 */
fun evaluateSecurity(f: SecurityFacts): List<SecurityCheck> = buildList {
    // 1 — access control.
    add(
        if (f.authEnabled)
            SecurityCheck("AUTH", "Access control enabled", CheckLevel.pass, "Authentication is required to connect.")
        else
            SecurityCheck(
                "AUTH", "Access control enabled", CheckLevel.fail,
                "The deployment accepts unauthenticated connections — anyone who can reach the port has full access.",
                "Start mongod with --auth (or security.authorization: enabled) and create an admin user.",
            ),
    )

    // 2 — TLS.
    add(
        when (f.tlsMode) {
            "requireTLS" -> SecurityCheck("TLS", "TLS required for connections", CheckLevel.pass, "All connections are encrypted (requireTLS).")
            "preferTLS", "allowTLS" -> SecurityCheck(
                "TLS", "TLS required for connections", CheckLevel.warn,
                "TLS is ${f.tlsMode} — unencrypted connections are still accepted.",
                "Set net.tls.mode to requireTLS once every client can speak TLS.",
            )
            "disabled", null -> SecurityCheck(
                "TLS", "TLS required for connections", CheckLevel.fail,
                "TLS is disabled — traffic, including credentials, crosses the network in clear text.",
                "Configure net.tls with a certificate and set mode to requireTLS.",
            )
            else -> SecurityCheck("TLS", "TLS required for connections", CheckLevel.info, "TLS mode: ${f.tlsMode}")
        },
    )

    // 3 — network exposure.
    add(
        if (f.bindIpAll)
            SecurityCheck(
                "BIND", "Network binding is restricted", CheckLevel.warn,
                "Bound to 0.0.0.0 — reachable on every interface. Safe only behind a firewall/VPC.",
                "Bind to specific interfaces (net.bindIp) or ensure a firewall limits the port.",
            )
        else
            SecurityCheck("BIND", "Network binding is restricted", CheckLevel.pass, "Bound to ${f.bindIp ?: "specific interfaces"}."),
    )

    // 4 — server currency (CVE proxy).
    val major = parseMajor(f.serverVersion)
    add(
        when {
            major == null -> SecurityCheck("VER", "Server version is current", CheckLevel.info, "Version ${f.serverVersion}.")
            major < LATEST_MAJOR - 2 -> SecurityCheck(
                "VER", "Server version is current", CheckLevel.fail,
                "Version ${f.serverVersion} is well behind current ($LATEST_MAJOR.x) and likely past end-of-life — unpatched CVEs.",
                "Plan an upgrade toward a supported major release.",
            )
            major < LATEST_MAJOR -> SecurityCheck(
                "VER", "Server version is current", CheckLevel.warn,
                "Version ${f.serverVersion} trails current ($LATEST_MAJOR.x). Check it is still in support.",
                "Track the MongoDB lifecycle schedule and plan upgrades.",
            )
            else -> SecurityCheck("VER", "Server version is current", CheckLevel.pass, "Version ${f.serverVersion}.")
        },
    )

    // 5 — superuser sprawl.
    add(
        when {
            !f.authEnabled -> SecurityCheck("SUPER", "Few superuser accounts", CheckLevel.info, "N/A without access control.")
            f.superuserCount == 0 -> SecurityCheck("SUPER", "Few superuser accounts", CheckLevel.info, "No estate-wide superusers visible (or insufficient privilege to list).")
            f.superuserCount <= 2 -> SecurityCheck("SUPER", "Few superuser accounts", CheckLevel.pass, "${f.superuserCount} superuser account(s).")
            else -> SecurityCheck(
                "SUPER", "Few superuser accounts", CheckLevel.warn,
                "${f.superuserCount} superuser accounts — broad blast radius if any is compromised.",
                "Grant least-privilege roles instead of root/*AnyDatabase; see the Security tab.",
            )
        },
    )

    // 6 — server-side JavaScript.
    f.javascriptEnabled?.let {
        add(
            if (it) SecurityCheck(
                "JS", "Server-side JavaScript disabled", CheckLevel.warn,
                "javascriptEnabled is true — \$where, mapReduce and \$function widen the injection surface.",
                "Set security.javascriptEnabled: false if no workload needs it.",
            )
            else SecurityCheck("JS", "Server-side JavaScript disabled", CheckLevel.pass, "Server-side JavaScript is disabled."),
        )
    }

    // 7 — cluster auth (replica sets / sharded).
    f.clusterAuthMode?.let {
        add(
            when (it) {
                "x509", "sendX509" -> SecurityCheck("CLUSTERAUTH", "Cluster members authenticate", CheckLevel.pass, "Cluster auth mode: $it.")
                "keyFile", "sendKeyFile" -> SecurityCheck(
                    "CLUSTERAUTH", "Cluster members authenticate", CheckLevel.warn,
                    "Members authenticate with a shared keyFile — fine internally, but x.509 is stronger.",
                    "Consider migrating internal auth to x.509 certificates.",
                )
                else -> SecurityCheck("CLUSTERAUTH", "Cluster members authenticate", CheckLevel.info, "Cluster auth mode: $it.")
            },
        )
    }

    // 8 — audit log (Enterprise).
    f.auditLogEnabled?.let {
        add(
            if (it) SecurityCheck("AUDIT", "Audit log enabled", CheckLevel.pass, "Auditing is configured.")
            else SecurityCheck("AUDIT", "Audit log enabled", CheckLevel.info, "No audit log (Enterprise-only feature).")
        )
    }
}

/** Rolls the checks into a 0–100 posture score — fails hurt most, warns less, info is neutral. */
fun securityScore(checks: List<SecurityCheck>): Int {
    val scored = checks.filter { it.level != CheckLevel.info }
    if (scored.isEmpty()) return 100
    val worst = scored.sumOf { c ->
        when (c.level) {
            CheckLevel.fail -> 100
            CheckLevel.warn -> 40
            else -> 0
        }.toInt()
    }
    return (100 - worst / scored.size).coerceIn(0, 100)
}

/* ============================ gather ============================ */

/** Collects security facts read-only. Anything the user can't observe is left null. */
fun gatherSecurityFacts(client: MongoClient): SecurityFacts {
    val admin = client.getDatabase("admin")

    val cmdLine = runCatching { admin.runCommand(Document("getCmdLineOpts", 1)) }.getOrNull()
    val parsed = (cmdLine?.get("parsed") as? Document) ?: Document()
    val security = (parsed["security"] as? Document) ?: Document()
    val net = (parsed["net"] as? Document) ?: Document()
    val tls = (net["tls"] as? Document) ?: (net["ssl"] as? Document) ?: Document()
    val setParam = (parsed["setParameter"] as? Document) ?: Document()

    val authEnabled = security.getString("authorization") == "enabled" ||
        // Fall back to probing: if listing users needs auth and we got here, infer from connectionStatus.
        runCatching {
            val status = admin.runCommand(Document("connectionStatus", 1))
            val authInfo = (status["authInfo"] as? Document)
            val users = (authInfo?.get("authenticatedUsers") as? List<*>).orEmpty()
            users.isNotEmpty()
        }.getOrDefault(false)

    val version = runCatching { admin.runCommand(Document("buildInfo", 1)).getString("version") }.getOrNull() ?: "?"

    val users = runCatching { listAllUsers(client) }.getOrDefault(emptyList())

    val bindIp = net.getString("bindIp")
    val bindIpAll = net["bindIpAll"] == true || bindIp == "0.0.0.0" || bindIp == "*" || bindIp == "::,0.0.0.0"

    val jsEnabled = (security["javascriptEnabled"] as? Boolean)
        ?: (setParam["javascriptEnabled"] as? Boolean)

    val auditLog = (parsed["auditLog"] as? Document)?.let { it.getString("destination") != null }

    return SecurityFacts(
        authEnabled = authEnabled,
        tlsMode = tls.getString("mode"),
        bindIp = bindIp,
        bindIpAll = bindIpAll,
        serverVersion = version,
        userCount = users.size,
        superuserCount = users.count { it.superuser },
        usersWithoutTls = false,
        javascriptEnabled = jsEnabled,
        auditLogEnabled = auditLog,
        clusterAuthMode = security.getString("clusterAuthMode"),
    )
}
