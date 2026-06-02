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

fun runImport(client: MongoClient, req: ImportRequest): ImportResult {
    val t0 = System.nanoTime()
    return runCatching {
        val docs = readDocs(req)
        var inserted = 0
        if (!req.dryRun && docs.isNotEmpty()) {
            val coll = client.getDatabase(req.db).getCollection(req.collection)
            docs.chunked(BATCH).forEach { batch ->
                val r = coll.insertMany(batch, InsertManyOptions().ordered(req.ordered))
                inserted += r.insertedIds.size
            }
        }
        ImportResult(ok = true, read = docs.size, inserted = inserted, durationMs = (System.nanoTime() - t0) / 1_000_000)
    }.getOrElse {
        ImportResult(ok = false, error = it.message, durationMs = (System.nanoTime() - t0) / 1_000_000)
    }
}

private fun readDocs(req: ImportRequest): List<Document> = when (req.format) {
    ImportFormat.ndjson ->
        Files.readAllLines(req.source).filter { it.isNotBlank() }
            .map { Document.parse(expandShellSyntax(it)) }
    ImportFormat.json -> {
        val text = Files.readString(req.source).trim()
        require(text.startsWith("[")) { "Top-level JSON must be an array." }
        @Suppress("UNCHECKED_CAST")
        val parsed = Document.parse("""{"_": $text}""")["_"] as List<Document>
        parsed
    }
    ImportFormat.csv -> {
        val lines = Files.readAllLines(req.source).filter { it.isNotBlank() }
        if (lines.size < 2) emptyList() else {
            val headers = parseCsvLine(lines[0])
            lines.drop(1).map { line ->
                val cells = parseCsvLine(line)
                Document().apply {
                    headers.forEachIndexed { i, h -> this[h] = cells.getOrNull(i) ?: "" }
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
