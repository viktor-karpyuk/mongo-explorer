package io.mex.mongo

import com.mongodb.client.MongoClient
import org.bson.Document
import org.bson.types.Binary
import org.bson.types.Decimal128
import org.bson.types.MaxKey
import org.bson.types.MinKey
import org.bson.types.ObjectId
import java.util.Date

private const val MAX_DISTINCT = 64

data class SchemaField(
    val path: String,
    val presence: Double,        // 0-1
    val types: Map<String, Double>, // sums to 1
    val approxDistinct: Int,
    /** Sampled distinct values, retained only for low-cardinality fields (autocomplete). */
    val sampleValues: List<String> = emptyList(),
) {
    /** The type that dominates this field across the sample, for display next to a suggestion. */
    val dominantType: String get() = types.maxByOrNull { it.value }?.key ?: "mixed"
}

data class SchemaReport(val sampleSize: Int, val totalDocs: Long, val fields: List<SchemaField>)

private class Stat {
    var presenceCount = 0
    val types = HashMap<String, Int>()
    val values = HashSet<String>()
}

fun analyzeSchema(client: MongoClient, db: String, coll: String, sampleSize: Int = 1000): SchemaReport {
    val collection = client.getDatabase(db).getCollection(coll)
    val total = collection.estimatedDocumentCount()
    val sz = minOf(sampleSize, maxOf(total.toInt(), 1))
    val docs = collection.aggregate(listOf(Document("\$sample", Document("size", sz)))).toList()
    val stats = HashMap<String, Stat>()
    for (doc in docs) walk(doc, "", stats)
    val fields = stats.entries.map { (path, stat) ->
        val total = stat.types.values.sum().coerceAtLeast(1)
        SchemaField(
            path = path,
            presence = if (docs.isEmpty()) 0.0 else stat.presenceCount.toDouble() / docs.size,
            types = stat.types.mapValues { it.value.toDouble() / total },
            approxDistinct = stat.values.size,
            // Enumerable fields get their values kept so the query editor can suggest them;
            // anything near the distinct cap is effectively free-form and not worth listing.
            sampleValues = if (stat.values.size < MAX_DISTINCT) stat.values.sorted() else emptyList(),
        )
    }.sortedByDescending { it.presence }
    return SchemaReport(sampleSize = docs.size, totalDocs = total, fields = fields)
}

private fun walk(value: Any?, prefix: String, stats: MutableMap<String, Stat>) {
    if (value == null) {
        bump(stats, prefix, "null", null); return
    }
    when (value) {
        is List<*> -> {
            bump(stats, prefix, "array", value)
            value.firstOrNull()?.let { walk(it, "$prefix[]", stats) }
        }
        is Document -> {
            bump(stats, prefix, "object", value)
            for ((k, v) in value) walk(v, if (prefix.isEmpty()) k else "$prefix.$k", stats)
        }
        is String -> bump(stats, prefix, "string", value)
        is Boolean -> bump(stats, prefix, "boolean", value)
        is Number -> bump(stats, prefix, "number", value)
        is ObjectId -> bump(stats, prefix, "objectid", value)
        is Date -> bump(stats, prefix, "date", value)
        is Decimal128 -> bump(stats, prefix, "decimal", value)
        is Binary -> bump(stats, prefix, "binary", value)
        is MinKey -> bump(stats, prefix, "minkey", value)
        is MaxKey -> bump(stats, prefix, "maxkey", value)
        else -> bump(stats, prefix, value::class.java.simpleName.lowercase(), value)
    }
}

private fun bump(stats: MutableMap<String, Stat>, path: String, type: String, v: Any?) {
    if (path.isEmpty()) return
    val s = stats.getOrPut(path) { Stat() }
    s.presenceCount++
    s.types[type] = (s.types[type] ?: 0) + 1
    if (s.values.size < MAX_DISTINCT) s.values.add(v?.toString() ?: "null")
}
