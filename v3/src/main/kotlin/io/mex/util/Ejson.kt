package io.mex.util

import kotlinx.serialization.json.*

enum class EjsonType { String, Number, Boolean, Null, ObjectId, Date, Long, Decimal, Binary, Regex, MinKey, MaxKey, ArrayT, ObjectT, Unknown }

data class FormattedValue(val label: String, val type: EjsonType)

fun parseRow(s: String): JsonObject? = runCatching {
    Json.parseToJsonElement(s).jsonObject
}.getOrNull()

fun formatValue(v: JsonElement): FormattedValue = when (v) {
    is JsonNull -> FormattedValue("null", EjsonType.Null)
    is JsonPrimitive -> when {
        v.isString -> FormattedValue(v.content, EjsonType.String)
        v.booleanOrNull != null -> FormattedValue(v.content, EjsonType.Boolean)
        else -> FormattedValue(v.content, EjsonType.Number)
    }
    is JsonArray -> FormattedValue("[ ${v.size} ]", EjsonType.ArrayT)
    is JsonObject -> formatObject(v)
}

private fun formatObject(o: JsonObject): FormattedValue {
    if ("\$oid" in o) return FormattedValue("ObjectId(\"${(o["\$oid"] as JsonPrimitive).content}\")", EjsonType.ObjectId)
    if ("\$date" in o) {
        val raw = o["\$date"]!!
        val text = when (raw) {
            is JsonPrimitive -> raw.content
            is JsonObject -> (raw["\$numberLong"] as? JsonPrimitive)?.content?.let {
                java.time.Instant.ofEpochMilli(it.toLong()).toString()
            } ?: "?"
            else -> "?"
        }
        return FormattedValue("ISODate(\"$text\")", EjsonType.Date)
    }
    if ("\$numberLong" in o) return FormattedValue("NumberLong(${(o["\$numberLong"] as JsonPrimitive).content})", EjsonType.Long)
    if ("\$numberDecimal" in o) return FormattedValue("NumberDecimal(\"${(o["\$numberDecimal"] as JsonPrimitive).content}\")", EjsonType.Decimal)
    if ("\$binary" in o) {
        val b = o["\$binary"] as? JsonObject ?: return FormattedValue("BinData(…)", EjsonType.Binary)
        val sub = (b["subType"] as? JsonPrimitive)?.content ?: "0"
        val b64 = (b["base64"] as? JsonPrimitive)?.content ?: ""
        return FormattedValue("BinData($sub, ${b64.take(12)}…)", EjsonType.Binary)
    }
    if ("\$regularExpression" in o) {
        val r = o["\$regularExpression"] as? JsonObject ?: return FormattedValue("Regex(…)", EjsonType.Regex)
        val p = (r["pattern"] as? JsonPrimitive)?.content ?: ""
        val opt = (r["options"] as? JsonPrimitive)?.content ?: ""
        return FormattedValue("/$p/$opt", EjsonType.Regex)
    }
    if ("\$minKey" in o) return FormattedValue("MinKey()", EjsonType.MinKey)
    if ("\$maxKey" in o) return FormattedValue("MaxKey()", EjsonType.MaxKey)
    return FormattedValue("{ ${o.size} field${if (o.size == 1) "" else "s"} }", EjsonType.ObjectT)
}

fun isWrapper(o: JsonObject): Boolean =
    "\$oid" in o || "\$date" in o || "\$numberLong" in o || "\$numberDecimal" in o ||
        "\$binary" in o || "\$regularExpression" in o || "\$minKey" in o || "\$maxKey" in o

fun collectColumns(rows: List<String>): List<String> {
    val seen = LinkedHashSet<String>()
    for (row in rows) {
        parseRow(row)?.keys?.forEach { seen.add(it) }
    }
    return seen.toList()
}

fun prettyPrint(s: String): String = runCatching {
    val element = Json.parseToJsonElement(s)
    Json { prettyPrint = true; prettyPrintIndent = "  " }.encodeToString(JsonElement.serializer(), element)
}.getOrDefault(s)
