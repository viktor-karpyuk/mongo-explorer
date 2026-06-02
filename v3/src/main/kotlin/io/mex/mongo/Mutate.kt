package io.mex.mongo

import com.mongodb.client.MongoClient
import com.mongodb.client.model.*
import org.bson.Document

data class MutateResult(
    val ok: Boolean,
    val matched: Long? = null,
    val modified: Long? = null,
    val inserted: Long? = null,
    val deleted: Long? = null,
    val durationMs: Long,
    val error: String? = null,
)

private fun <T> timed(block: () -> T): Pair<T, Long> {
    val t0 = System.nanoTime()
    val v = block()
    return v to (System.nanoTime() - t0) / 1_000_000
}

private fun parseDocs(input: String): List<Document> {
    val trimmed = input.trim()
    if (trimmed.isEmpty()) return emptyList()
    if (trimmed.startsWith("[")) {
        // JSON array
        @Suppress("UNCHECKED_CAST")
        val arr = Document.parse("""{"_": $trimmed}""")["_"] as List<Document>
        return arr
    }
    // NDJSON
    return trimmed.split("\n").mapNotNull { line ->
        line.trim().takeIf { it.isNotEmpty() }?.let { Document.parse(expandShellSyntax(it)) }
    }
}

fun insertOne(client: MongoClient, db: String, coll: String, document: String): MutateResult {
    return runCatching {
        val (r, ms) = timed {
            client.getDatabase(db).getCollection(coll).insertOne(parseDocument(document))
        }
        MutateResult(ok = true, inserted = if (r.wasAcknowledged()) 1 else 0, durationMs = ms)
    }.getOrElse { MutateResult(ok = false, error = it.message, durationMs = 0) }
}

fun insertMany(client: MongoClient, db: String, coll: String, documents: String, ordered: Boolean = true): MutateResult {
    return runCatching {
        val docs = parseDocs(documents)
        require(docs.isNotEmpty()) { "No documents to insert." }
        val (r, ms) = timed {
            client.getDatabase(db).getCollection(coll)
                .insertMany(docs, InsertManyOptions().ordered(ordered))
        }
        MutateResult(ok = true, inserted = r.insertedIds.size.toLong(), durationMs = ms)
    }.getOrElse { MutateResult(ok = false, error = it.message, durationMs = 0) }
}

fun updateDocs(client: MongoClient, db: String, coll: String, filter: String, update: String, many: Boolean, upsert: Boolean): MutateResult {
    return runCatching {
        val f = parseFilter(filter)
        val u = parseDocument(update)
        val opts = UpdateOptions().upsert(upsert)
        val (r, ms) = timed {
            val collection = client.getDatabase(db).getCollection(coll)
            if (many) collection.updateMany(f, u, opts) else collection.updateOne(f, u, opts)
        }
        MutateResult(ok = true, matched = r.matchedCount, modified = r.modifiedCount, durationMs = ms)
    }.getOrElse { MutateResult(ok = false, error = it.message, durationMs = 0) }
}

fun replaceOne(client: MongoClient, db: String, coll: String, filter: String, replacement: String, upsert: Boolean): MutateResult {
    return runCatching {
        val (r, ms) = timed {
            client.getDatabase(db).getCollection(coll).replaceOne(
                parseFilter(filter),
                parseDocument(replacement),
                ReplaceOptions().upsert(upsert),
            )
        }
        MutateResult(ok = true, matched = r.matchedCount, modified = r.modifiedCount, durationMs = ms)
    }.getOrElse { MutateResult(ok = false, error = it.message, durationMs = 0) }
}

fun deleteDocs(client: MongoClient, db: String, coll: String, filter: String, many: Boolean): MutateResult {
    return runCatching {
        val (r, ms) = timed {
            val c = client.getDatabase(db).getCollection(coll)
            if (many) c.deleteMany(parseFilter(filter)) else c.deleteOne(parseFilter(filter))
        }
        MutateResult(ok = true, deleted = r.deletedCount, durationMs = ms)
    }.getOrElse { MutateResult(ok = false, error = it.message, durationMs = 0) }
}
