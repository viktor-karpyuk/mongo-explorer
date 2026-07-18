package io.mex.util

import java.net.URI

data class ParsedUri(
    val protocol: String,
    val host: String,
    val port: Int?,
    val database: String?,
)

fun parseMongoUri(uri: String): ParsedUri? = runCatching {
    val parsed = URI(uri)
    val scheme = parsed.scheme ?: return null
    if (scheme != "mongodb" && scheme != "mongodb+srv") return null
    ParsedUri(
        protocol = scheme,
        host = parsed.host ?: "",
        port = parsed.port.takeIf { it >= 0 },
        database = parsed.path?.removePrefix("/")?.takeIf { it.isNotEmpty() },
    )
}.getOrNull()

fun formatUriPreview(uri: String): String {
    // Multi-host URIs (host1,host2) defeat java.net.URI, so the fallback must
    // still strip credentials rather than echo the raw URI.
    val p = parseMongoUri(uri) ?: return redactCredentials(uri)
    val port = p.port?.let { ":$it" } ?: ""
    val db = p.database?.let { "/$it" } ?: ""
    return "${p.protocol}://${p.host}$port$db"
}

fun redactCredentials(uri: String): String =
    Regex("(mongodb(\\+srv)?://)([^@/]+@)").replace(uri, "$1***@")
