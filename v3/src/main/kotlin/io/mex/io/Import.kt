package io.mex.io

import com.mongodb.client.MongoClient
import com.mongodb.client.model.InsertManyOptions
import io.mex.mongo.expandShellSyntax
import org.bson.Document
import java.nio.file.Files
import java.nio.file.Path

enum class ImportFormat { json, ndjson, csv }

data class ImportRequest(
    val db: String,
    val collection: String,
    val format: ImportFormat,
    val source: Path,
    val dryRun: Boolean,
    val ordered: Boolean = true,
)

data class ImportResult(
    val ok: Boolean,
    val read: Int = 0,
    val inserted: Int = 0,
    val durationMs: Long,
    val error: String? = null,
)

private const val BATCH = 1000

/**
 * Streaming: documents are parsed and inserted in [BATCH]-sized windows. The previous
 * implementation materialised the whole file *and* every parsed Document before the
 * first insert — a 1 GB NDJSON file needed several GB of heap and OOM'd; the working
 * set is now one batch.
 */
fun runImport(client: MongoClient, req: ImportRequest): ImportResult {
    val t0 = System.nanoTime()
    return runCatching {
        val coll = client.getDatabase(req.db).getCollection(req.collection)
        var read = 0
        var inserted = 0
        val batch = ArrayList<Document>(BATCH)
        fun flush() {
            if (batch.isEmpty()) return
            if (!req.dryRun) {
                val r = coll.insertMany(batch, InsertManyOptions().ordered(req.ordered))
                inserted += r.insertedIds.size
            }
            batch.clear()
        }
        forEachDoc(req) { doc ->
            read++
            batch += doc
            if (batch.size >= BATCH) flush()
        }
        flush()
        ImportResult(ok = true, read = read, inserted = inserted, durationMs = (System.nanoTime() - t0) / 1_000_000)
    }.getOrElse {
        ImportResult(ok = false, error = it.message, durationMs = (System.nanoTime() - t0) / 1_000_000)
    }
}

private fun forEachDoc(req: ImportRequest, emit: (Document) -> Unit) {
    when (req.format) {
        ImportFormat.ndjson ->
            Files.newBufferedReader(req.source).useLines { lines ->
                lines.filter { it.isNotBlank() }.forEach { emit(Document.parse(expandShellSyntax(it))) }
            }
        ImportFormat.json ->
            // Incremental EJSON array reader — no whole-file string, no wrapper document.
            Files.newBufferedReader(req.source).use { r ->
                val reader = org.bson.json.JsonReader(r)
                val codec = org.bson.codecs.DocumentCodec()
                val ctx = org.bson.codecs.DecoderContext.builder().build()
                reader.readStartArray()
                while (reader.readBsonType() != org.bson.BsonType.END_OF_DOCUMENT) {
                    emit(codec.decode(reader, ctx))
                }
                reader.readEndArray()
            }
        ImportFormat.csv ->
            Files.newBufferedReader(req.source).useLines { lines ->
                var headers: List<String>? = null
                lines.filter { it.isNotBlank() }.forEach { line ->
                    val h = headers
                    if (h == null) {
                        headers = parseCsvLine(line)
                    } else {
                        val cells = parseCsvLine(line)
                        emit(Document().apply { h.forEachIndexed { i, k -> this[k] = cells.getOrNull(i) ?: "" } })
                    }
                }
            }
    }
}

private fun parseCsvLine(line: String): List<String> {
    val out = mutableListOf<String>()
    val buf = StringBuilder()
    var inQuote = false
    var i = 0
    while (i < line.length) {
        val ch = line[i]
        if (inQuote) when {
            ch == '"' && i + 1 < line.length && line[i + 1] == '"' -> { buf.append('"'); i++ }
            ch == '"' -> inQuote = false
            else -> buf.append(ch)
        } else when (ch) {
            ',' -> { out += buf.toString(); buf.setLength(0) }
            '"' -> inQuote = true
            else -> buf.append(ch)
        }
        i++
    }
    out += buf.toString()
    return out
}
