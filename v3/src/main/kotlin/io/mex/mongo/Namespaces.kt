package io.mex.mongo

import com.mongodb.client.MongoClient
import org.bson.Document

data class DatabaseInfo(val name: String, val sizeOnDisk: Long?, val empty: Boolean?)

enum class CollectionType { collection, view, timeseries }

data class CollectionInfo(
    val name: String,
    val type: CollectionType,
    val options: Map<String, Any?>,
)

data class DbStats(
    val db: String,
    val collections: Long,
    val views: Long,
    val objects: Long,
    val avgObjSize: Double,
    val dataSize: Long,
    val storageSize: Long,
    val indexes: Long,
    val indexSize: Long,
    val totalSize: Long,
)

data class CollStats(
    val ns: String,
    val count: Long,
    val size: Long,
    val avgObjSize: Double,
    val storageSize: Long,
    val totalIndexSize: Long,
    val nindexes: Long,
    val capped: Boolean,
)

data class CreateCollectionInput(
    val name: String,
    val cappedSize: Long? = null,
    val cappedMax: Long? = null,
    val timeseriesField: String? = null,
)

fun listDatabases(client: MongoClient): List<DatabaseInfo> {
    val result = client.getDatabase("admin").runCommand(Document("listDatabases", 1))
    @Suppress("UNCHECKED_CAST")
    val dbs = (result["databases"] as? List<Document>).orEmpty()
    return dbs.map {
        DatabaseInfo(
            name = it.getString("name"),
            sizeOnDisk = it.get("sizeOnDisk") as? Long,
            empty = it["empty"] as? Boolean,
        )
    }
}

fun listCollections(client: MongoClient, db: String): List<CollectionInfo> {
    val list = client.getDatabase(db).listCollections().toList()
    return list.map { doc ->
        val opts = (doc["options"] as? Document)?.toMap().orEmpty()
        val type = when {
            doc.getString("type") == "view" -> CollectionType.view
            opts.containsKey("timeseries") -> CollectionType.timeseries
            else -> CollectionType.collection
        }
        CollectionInfo(name = doc.getString("name"), type = type, options = opts)
    }
}

fun dbStats(client: MongoClient, db: String): DbStats {
    val s = client.getDatabase(db).runCommand(Document("dbStats", 1).append("scale", 1))
    return DbStats(
        db = db,
        collections = s.numberOr("collections"),
        views = s.numberOr("views"),
        objects = s.numberOr("objects"),
        avgObjSize = (s["avgObjSize"] as? Number)?.toDouble() ?: 0.0,
        dataSize = s.numberOr("dataSize"),
        storageSize = s.numberOr("storageSize"),
        indexes = s.numberOr("indexes"),
        indexSize = s.numberOr("indexSize"),
        totalSize = s.numberOr("totalSize"),
    )
}

fun collStats(client: MongoClient, db: String, collection: String): CollStats {
    val s = client.getDatabase(db)
        .runCommand(Document("collStats", collection).append("scale", 1))
    return CollStats(
        ns = "$db.$collection",
        count = s.numberOr("count"),
        size = s.numberOr("size"),
        avgObjSize = (s["avgObjSize"] as? Number)?.toDouble() ?: 0.0,
        storageSize = s.numberOr("storageSize"),
        totalIndexSize = s.numberOr("totalIndexSize"),
        nindexes = s.numberOr("nindexes"),
        capped = (s["capped"] as? Boolean) ?: false,
    )
}

/**
 * MongoDB has no explicit create-database command and does not list empty
 * databases, so a database only becomes visible once it holds a collection.
 * The initial collection is therefore kept, not dropped.
 */
fun createDatabase(client: MongoClient, db: String, initialCollection: String) {
    client.getDatabase(db).createCollection(initialCollection)
}

fun dropDatabase(client: MongoClient, db: String) {
    client.getDatabase(db).drop()
}

fun createCollection(client: MongoClient, db: String, input: CreateCollectionInput) {
    val options = com.mongodb.client.model.CreateCollectionOptions()
    if (input.cappedSize != null) {
        options.capped(true).sizeInBytes(input.cappedSize)
        if (input.cappedMax != null) options.maxDocuments(input.cappedMax)
    }
    if (input.timeseriesField != null) {
        options.timeSeriesOptions(
            com.mongodb.client.model.TimeSeriesOptions(input.timeseriesField),
        )
    }
    client.getDatabase(db).createCollection(input.name, options)
}

fun dropCollection(client: MongoClient, db: String, name: String) {
    client.getDatabase(db).getCollection(name).drop()
}

fun renameCollection(client: MongoClient, db: String, from: String, to: String) {
    val ns = com.mongodb.MongoNamespace(db, to)
    client.getDatabase(db).getCollection(from).renameCollection(ns)
}

private fun Document.numberOr(key: String): Long = (this[key] as? Number)?.toLong() ?: 0L
