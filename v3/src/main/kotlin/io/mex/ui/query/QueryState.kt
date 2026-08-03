package io.mex.ui.query

import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateMapOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import io.mex.AppContext
import io.mex.data.QueryHistoryInput
import io.mex.data.QueryKind
import io.mex.mongo.FindRequest
import io.mex.mongo.FindResult
import io.mex.mongo.MongoRegistry
import io.mex.mongo.SchemaField
import io.mex.mongo.analyzeSchema
import io.mex.mongo.executeCount
import io.mex.mongo.executeFind
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

class QueryDraft {
    var filter by mutableStateOf("{}")
    var projection by mutableStateOf("")
    var sort by mutableStateOf("")
    var skip by mutableStateOf(0)
    var limit by mutableStateOf(50)
    var maxTimeMs by mutableStateOf(30_000L)
}

/**
 * Aggregation editor state, hoisted app-wide like [QueryDraft] — as view-local
 * `remember`s, peeking at Schema/Indexes destroyed the pipeline being authored.
 */
class AggregationDraft {
    val stages = androidx.compose.runtime.mutableStateListOf(io.mex.mongo.StageDraft())
    var limit by mutableStateOf(50)
    var maxTimeMs by mutableStateOf(60_000L)
    var result by mutableStateOf<FindResult?>(null)
    var running by mutableStateOf(false)
    var skip by mutableStateOf(0)
    var lastUpTo by mutableStateOf(-1)
}

class QueryStore(private val ctx: AppContext, private val registry: MongoRegistry) {
    private val drafts = mutableStateMapOf<String, QueryDraft>()
    private val aggregations = mutableStateMapOf<String, AggregationDraft>()
    private val results = mutableStateMapOf<String, FindResult>()
    private val running = mutableStateMapOf<String, Boolean>()
    private val totals = mutableStateMapOf<String, Long>()
    private val schemas = mutableStateMapOf<String, List<SchemaField>>()

    fun draft(key: String): QueryDraft = drafts.getOrPut(key) { QueryDraft() }
    fun aggregation(key: String): AggregationDraft = aggregations.getOrPut(key) { AggregationDraft() }
    fun result(key: String): FindResult? = results[key]
    fun running(key: String): Boolean = running[key] == true
    fun total(key: String): Long? = totals[key]

    /** Sampled field paths backing query autocomplete; empty until [loadSchema] finishes. */
    fun schema(key: String): List<SchemaField> = schemas[key].orEmpty()

    /**
     * Samples the collection once per namespace so the editor can suggest field paths.
     * Deliberately small — this runs when a collection is opened, not per keystroke.
     */
    suspend fun loadSchema(key: String, connectionId: String, db: String, collection: String) {
        if (schemas.containsKey(key)) return
        val client = registry.client(connectionId) ?: return
        val report = withContext(Dispatchers.IO) {
            runCatching { analyzeSchema(client, db, collection, sampleSize = 200) }.getOrNull()
        }
        schemas[key] = report?.fields.orEmpty()
    }

    /** Refreshes the exact matched count for the draft's filter. Cheap for indexed/empty filters. */
    suspend fun refreshCount(key: String, connectionId: String, db: String, collection: String) {
        val client = registry.client(connectionId) ?: return
        val d = draft(key)
        val n = withContext(Dispatchers.IO) {
            executeCount(
                client,
                FindRequest(
                    connectionId = connectionId,
                    db = db,
                    collection = collection,
                    filter = d.filter,
                    maxTimeMs = d.maxTimeMs,
                ),
            )
        }
        if (n != null) totals[key] = n else totals.remove(key)
    }

    suspend fun run(key: String, connectionId: String, db: String, collection: String) {
        val client = registry.client(connectionId) ?: return
        val d = draft(key)
        running[key] = true
        try {
            val res = withContext(Dispatchers.IO) {
                executeFind(
                    client,
                    FindRequest(
                        connectionId = connectionId,
                        db = db,
                        collection = collection,
                        filter = d.filter,
                        projection = d.projection,
                        sort = d.sort,
                        skip = d.skip,
                        limit = d.limit,
                        maxTimeMs = d.maxTimeMs,
                    ),
                )
            }
            results[key] = res
            // JDBC INSERT + trim DELETE — keep it off the main dispatcher (it ran there
            // after the find's withContext block ended, adding jank to every Run).
            withContext(Dispatchers.IO) {
                ctx.queryHistory.record(
                    QueryHistoryInput(
                        connectionId = connectionId,
                        database = db,
                        collection = collection,
                        kind = QueryKind.find,
                        body = buildJsonObject {
                            put("filter", d.filter)
                            put("projection", d.projection)
                            put("sort", d.sort)
                            put("skip", d.skip)
                            put("limit", d.limit)
                        }.toString(),
                        durationMs = if (res is FindResult.Ok) res.durationMs else (res as FindResult.Failed).durationMs,
                        rowCount = if (res is FindResult.Ok) res.rows.size else null,
                        error = if (res is FindResult.Failed) res.error else null,
                    ),
                )
            }
        } finally {
            running[key] = false
        }
    }

    suspend fun nextPage(key: String, connectionId: String, db: String, collection: String) {
        val d = draft(key)
        d.skip += d.limit
        run(key, connectionId, db, collection)
    }

    suspend fun prevPage(key: String, connectionId: String, db: String, collection: String) {
        val d = draft(key)
        d.skip = (d.skip - d.limit).coerceAtLeast(0)
        run(key, connectionId, db, collection)
    }

    suspend fun firstPage(key: String, connectionId: String, db: String, collection: String) {
        draft(key).skip = 0
        run(key, connectionId, db, collection)
    }

    /** Changing page size restarts from page one so the offset stays meaningful. */
    suspend fun setPageSize(key: String, size: Int, connectionId: String, db: String, collection: String) {
        val d = draft(key)
        d.limit = size.coerceAtLeast(1)
        d.skip = 0
        run(key, connectionId, db, collection)
    }
}

fun nsKey(connectionId: String, db: String, collection: String) =
    "$connectionId::$db.$collection"

/** The find parameters stored in a query_history row's `body` column. */
data class FindBody(
    val filter: String,
    val projection: String,
    val sort: String,
    val skip: Int?,
    val limit: Int?,
)

fun parseFindBody(body: String): FindBody? = runCatching {
    val o = Json.parseToJsonElement(body).jsonObject
    FindBody(
        filter = o["filter"]?.jsonPrimitive?.content ?: return null,
        projection = o["projection"]?.jsonPrimitive?.content ?: "",
        sort = o["sort"]?.jsonPrimitive?.content ?: "",
        skip = o["skip"]?.jsonPrimitive?.intOrNull,
        limit = o["limit"]?.jsonPrimitive?.intOrNull,
    )
}.getOrNull()
