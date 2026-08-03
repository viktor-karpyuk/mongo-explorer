package io.mex.mongo

import org.bson.Document
import org.bson.conversions.Bson

private val OBJECT_ID = Regex("""ObjectId\(\s*["']([0-9a-fA-F]{24})["']\s*\)""")
private val ISO_DATE = Regex("""ISODate\(\s*["']([^"']+)["']\s*\)""")
private val NUMBER_LONG = Regex("""NumberLong\(\s*"?([0-9-]+)"?\s*\)""")
private val NUMBER_DECIMAL = Regex("""NumberDecimal\(\s*["']?([0-9.eE+-]+)["']?\s*\)""")
private val TIMESTAMP = Regex("""Timestamp\(\s*(\d+)\s*,\s*(\d+)\s*\)""")
private val UUID_CTOR = Regex("""UUID\(\s*["']([^"']+)["']\s*\)""")

fun parseFilter(input: String): Bson {
    val trimmed = input.trim()
    if (trimmed.isEmpty()) return Document()
    // 24-hex shorthand for _id.
    if (Regex("^[a-fA-F0-9]{24}$").matches(trimmed)) {
        return Document.parse("""{ "_id": { "${'$'}oid": "$trimmed" } }""")
    }
    return Document.parse(expandShellSyntax(trimmed))
}

fun parseDocument(input: String): Document {
    val trimmed = input.trim()
    if (trimmed.isEmpty()) return Document()
    return Document.parse(expandShellSyntax(trimmed))
}

fun parseSort(input: String): Document = parseDocument(input)
fun parseProjection(input: String): Document = parseDocument(input)

/** Rewrites Mongo shell-flavored constructors into canonical EJSON. */
fun expandShellSyntax(input: String): String {
    var s = input
    s = OBJECT_ID.replace(s) { """{ "${'$'}oid": "${it.groupValues[1]}" }""" }
    s = ISO_DATE.replace(s) { """{ "${'$'}date": "${it.groupValues[1]}" }""" }
    s = NUMBER_LONG.replace(s) { """{ "${'$'}numberLong": "${it.groupValues[1]}" }""" }
    s = NUMBER_DECIMAL.replace(s) { """{ "${'$'}numberDecimal": "${it.groupValues[1]}" }""" }
    s = TIMESTAMP.replace(s) {
        """{ "${'$'}timestamp": { "t": ${it.groupValues[1]}, "i": ${it.groupValues[2]} } }"""
    }
    s = UUID_CTOR.replace(s) {
        // mongosh writes UUIDs as hyphenated hex; EJSON $binary wants the 16 raw bytes
        // base64-encoded — passing the hex through verbatim made Document.parse throw.
        val hex = it.groupValues[1].replace("-", "")
        val bytes = ByteArray(16) { i -> hex.substring(i * 2, i * 2 + 2).toInt(16).toByte() }
        val b64 = java.util.Base64.getEncoder().encodeToString(bytes)
        """{ "${'$'}binary": { "base64": "$b64", "subType": "04" } }"""
    }
    return s
}
