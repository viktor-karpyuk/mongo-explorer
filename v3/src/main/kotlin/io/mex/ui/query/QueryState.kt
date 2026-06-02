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
import io.mex.mongo.executeFind
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

class QueryDraft {
    var filter by mutableStateOf("{}")
    var projection by mutableStateOf("")
    var sort by mutableStateOf("")
    var skip by mutableStateOf(0)
    var limit by mutableStateOf(50)
    var maxTimeMs by mutableStateOf(30_000L)
}

class QueryStore(private val ctx: AppContext, private val registry: MongoRegistry) {
    private val drafts = mutableStateMapOf<String, QueryDraft>()
    private val results = mutableStateMapOf<String, FindResult>()
    private val running = mutableStateMapOf<String, Boolean>()

    fun draft(key: String): QueryDraft = drafts.getOrPut(key) { QueryDraft() }
    fun result(key: String): FindResult? = results[key]
    fun running(key: String): Boolean = running[key] == true

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
            ctx.queryHistory.record(
                QueryHistoryInput(
                    connectionId = connectionId,
                    database = db,
                    collection = collection,
                    kind = QueryKind.find,
                    body = """{"filter":"${d.filter}","projection":"${d.projection}","sort":"${d.sort}","skip":${d.skip},"limit":${d.limit}}""",
                    durationMs = if (res is FindResult.Ok) res.durationMs else (res as FindResult.Failed).durationMs,
                    rowCount = if (res is FindResult.Ok) res.rows.size else null,
                    error = if (res is FindResult.Failed) res.error else null,
                ),
            )
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
}

fun nsKey(connectionId: String, db: String, collection: String) =
    "$connectionId::$db.$collection"
