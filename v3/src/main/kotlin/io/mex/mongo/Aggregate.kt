package io.mex.mongo

import com.mongodb.client.MongoClient
import org.bson.Document
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import java.util.concurrent.TimeUnit

data class AggregateRequest(
    val connectionId: String,
    val db: String,
    val collection: String,
    val pipeline: List<Pair<String, String>>, // operator → body
    val limit: Int = 50,
    val maxTimeMs: Long = 60_000,
    /** Documents to skip, appended as a `$skip` stage so results can be paged. */
    val skip: Int = 0,
)

private val EJSON_SETTINGS: JsonWriterSettings = JsonWriterSettings.builder()
    .outputMode(JsonMode.EXTENDED)
    .build()

fun executeAggregate(client: MongoClient, req: AggregateRequest): FindResult {
    val t0 = System.nanoTime()
    return runCatching {
        val limit = req.limit.coerceAtLeast(1)
        val user = req.pipeline.map { (op, body) ->
            require(op.startsWith("$")) { "Stage operator must start with $: $op" }
            Document(op, parseDocument(body))
        }
        // Paging stages go after the user's pipeline so they window its output.
        val paging = buildList {
            if (req.skip > 0) add(Document("\$skip", req.skip))
            add(Document("\$limit", limit + 1))
        }
        val stages = user + paging

        var cursor = client.getDatabase(req.db).getCollection(req.collection)
            .aggregate(stages)
        if (req.maxTimeMs > 0) cursor = cursor.maxTime(req.maxTimeMs, TimeUnit.MILLISECONDS)

        val docs = cursor.into(mutableListOf<Document>())
        val hasMore = docs.size > limit
        val rows = (if (hasMore) docs.dropLast(1) else docs).map { it.toJson(EJSON_SETTINGS) }
        FindResult.Ok(rows = rows, hasMore = hasMore, durationMs = (System.nanoTime() - t0) / 1_000_000)
    }.getOrElse {
        FindResult.Failed(it.message ?: it::class.java.simpleName, (System.nanoTime() - t0) / 1_000_000)
    }
}
