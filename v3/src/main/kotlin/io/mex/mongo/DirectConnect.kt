package io.mex.mongo

/**
 * Rewrites a stored connection URI to talk to one specific node of the cluster it
 * points at, e.g. `mongodb://u:p@h1,h2/db?replicaSet=rs0` → `mongodb://u:p@h2/db?directConnection=true`.
 *
 * Rules, each load-bearing:
 * - `replicaSet` is dropped — it contradicts `directConnection` and the driver rejects the pair.
 * - `mongodb+srv` becomes plain `mongodb` (the node host is already resolved), which loses
 *   the SRV defaults, so `tls=true` and `authSource=admin` are added unless the query
 *   already pins them — silently downgrading Atlas to plaintext would be worse than wrong.
 * - SRV-only options (`srvMaxHosts`, `srvServiceName`) are dropped with the scheme.
 * - Credentials, auth db path and every other option round-trip untouched.
 */
fun directNodeUri(baseUri: String, host: String): String {
    val m = Regex("""^(mongodb(?:\+srv)?)://(?:([^@]+)@)?([^/?]+)(?:/([^?]*))?(?:\?(.*))?$""")
        .matchEntire(baseUri.trim())
        ?: throw IllegalArgumentException("Not a MongoDB URI")
    val (scheme, userinfo, _, pathDb, query) = m.destructured
    val wasSrv = scheme == "mongodb+srv"

    val params = query.split('&')
        .filter { it.isNotBlank() }
        .filterNot {
            val k = it.substringBefore('=').lowercase()
            k == "replicaset" || k == "directconnection" || k == "srvmaxhosts" || k == "srvservicename"
        }
        .toMutableList()
    fun has(key: String) = params.any { it.substringBefore('=').equals(key, ignoreCase = true) }
    if (wasSrv && !has("tls") && !has("ssl")) params += "tls=true"
    if (wasSrv && !has("authSource") && pathDb.isEmpty() && userinfo.isNotEmpty()) params += "authSource=admin"
    params += "directConnection=true"

    val auth = if (userinfo.isEmpty()) "" else "$userinfo@"
    val path = if (pathDb.isEmpty()) "/" else "/$pathDb"
    return "mongodb://$auth$host$path?${params.joinToString("&")}"
}
