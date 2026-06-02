package io.mex.mongo

import com.mongodb.client.MongoClient
import com.mongodb.ExplainVerbosity
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings

private val EJSON: JsonWriterSettings = JsonWriterSettings.builder()
    .outputMode(JsonMode.EXTENDED)
    .build()

fun explainFind(client: MongoClient, req: FindRequest): String {
    val filter = parseFilter(req.filter)
    val projection = parseProjection(req.projection)
    val sort = parseSort(req.sort)
    var cursor = client.getDatabase(req.db).getCollection(req.collection).find(filter)
    if (!projection.isEmpty) cursor = cursor.projection(projection)
    if (!sort.isEmpty) cursor = cursor.sort(sort)
    if (req.skip > 0) cursor = cursor.skip(req.skip)
    if (req.limit > 0) cursor = cursor.limit(req.limit)
    return cursor.explain(ExplainVerbosity.QUERY_PLANNER).toJson(EJSON)
}
