package io.mex.mongo

import com.mongodb.client.MongoClient
import org.bson.Document
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import java.util.concurrent.TimeUnit

data class FindRequest(
    val connectionId: String,
    val db: String,
    val collection: String,
    val filter: String = "",
    val projection: String = "",
    val sort: String = "",
    val skip: Int = 0,
    val limit: Int = 50,
    val maxTimeMs: Long = 30_000,
)

sealed class FindResult {
    data class Ok(
        val rows: List<String>, // canonical EJSON
        val hasMore: Boolean,
        val durationMs: Long,
    ) : FindResult()
    data class Failed(val error: String, val durationMs: Long) : FindResult()
}

private val EJSON_SETTINGS: JsonWriterSettings = JsonWriterSettings.builder()
    .outputMode(JsonMode.EXTENDED)
    .build()

fun executeFind(client: MongoClient, req: FindRequest): FindResult {
    val t0 = System.nanoTime()
    return runCatching {
        val filter = parseFilter(req.filter)
        val projection = parseProjection(req.projection)
        val sort = parseSort(req.sort)
        val limit = req.limit.coerceAtLeast(1)
        val pageFetch = limit + 1

        var cursor = client.getDatabase(req.db).getCollection(req.collection).find(filter)
        if (!projection.isEmpty) cursor = cursor.projection(projection)
        if (!sort.isEmpty) cursor = cursor.sort(sort)
        if (req.skip > 0) cursor = cursor.skip(req.skip)
        cursor = cursor.limit(pageFetch)
        if (req.maxTimeMs > 0) cursor = cursor.maxTime(req.maxTimeMs, TimeUnit.MILLISECONDS)

        val docs = cursor.into(mutableListOf<Document>())
        val hasMore = docs.size > limit
        val rows = (if (hasMore) docs.dropLast(1) else docs).map { it.toJson(EJSON_SETTINGS) }
        FindResult.Ok(
            rows = rows,
            hasMore = hasMore,
            durationMs = (System.nanoTime() - t0) / 1_000_000,
        )
    }.getOrElse {
        FindResult.Failed(
            error = it.message ?: it::class.java.simpleName,
            durationMs = (System.nanoTime() - t0) / 1_000_000,
        )
    }
}
