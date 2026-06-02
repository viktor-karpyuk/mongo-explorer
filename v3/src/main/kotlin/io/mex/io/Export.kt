package io.mex.io

import com.mongodb.client.MongoClient
import io.mex.mongo.parseFilter
import io.mex.mongo.parseProjection
import io.mex.mongo.parseSort
import org.bson.Document
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import java.io.BufferedWriter
import java.nio.file.Files
import java.nio.file.Path

enum class ExportFormat { json, ndjson, csv }

data class ExportRequest(
    val connectionId: String,
    val db: String,
    val collection: String,
    val format: ExportFormat,
    val filter: String = "",
    val projection: String = "",
    val sort: String = "",
    val limit: Int = 0,
    val target: Path,
)

data class ExportResult(
    val ok: Boolean,
    val written: Long = 0,
    val durationMs: Long,
    val error: String? = null,
)

private val EJSON: JsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build()

fun exportFind(client: MongoClient, req: ExportRequest): ExportResult {
    val t0 = System.nanoTime()
    return runCatching {
        val filter = parseFilter(req.filter)
        val projection = parseProjection(req.projection)
        val sort = parseSort(req.sort)
        var cursor = client.getDatabase(req.db).getCollection(req.collection).find(filter)
        if (!projection.isEmpty) cursor = cursor.projection(projection)
        if (!sort.isEmpty) cursor = cursor.sort(sort)
        if (req.limit > 0) cursor = cursor.limit(req.limit)

        Files.newBufferedWriter(req.target).use { out ->
            var written = 0L
            var headers: List<String>? = null
            if (req.format == ExportFormat.json) out.write("[\n")

            for (doc in cursor) {
                when (req.format) {
                    ExportFormat.ndjson -> { out.write(doc.toJson(EJSON)); out.write("\n") }
                    ExportFormat.json -> {
                        if (written > 0) out.write(",\n")
                        out.write(doc.toJson(EJSON))
                    }
                    ExportFormat.csv -> {
                        if (headers == null) {
                            headers = doc.keys.toList()
                            out.write(headers!!.joinToString(",") { csvQuote(it) })
                            out.write("\n")
                        }
                        out.write(headers!!.joinToString(",") { key -> csvQuote(stringifyCell(doc[key])) })
                        out.write("\n")
                    }
                }
                written++
            }
            if (req.format == ExportFormat.json) out.write("\n]\n")
            ExportResult(ok = true, written = written, durationMs = (System.nanoTime() - t0) / 1_000_000)
        }
    }.getOrElse {
        ExportResult(ok = false, error = it.message, durationMs = (System.nanoTime() - t0) / 1_000_000)
    }
}

private fun csvQuote(s: String): String {
    return if (Regex("[\",\\n\\r]").containsMatchIn(s)) "\"${s.replace("\"", "\"\"")}\"" else s
}

private fun stringifyCell(v: Any?): String = when (v) {
    null -> ""
    is String -> v
    is Document -> v.toJson(EJSON)
    is List<*> -> v.toString()
    else -> v.toString()
}

private fun BufferedWriter.write(d: Document) = this.write(d.toJson(EJSON))
