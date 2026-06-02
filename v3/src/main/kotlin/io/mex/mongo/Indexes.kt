package io.mex.mongo

import com.mongodb.client.MongoClient
import com.mongodb.client.model.IndexOptions
import org.bson.Document

data class IndexKey(val field: String, val direction: String)

data class IndexInfo(
    val name: String,
    val keys: List<IndexKey>,
    val unique: Boolean,
    val sparse: Boolean,
    val background: Boolean,
    val ttlSeconds: Long?,
    val partialFilter: String?,
)

data class CreateIndexInput(
    val keys: List<IndexKey>,
    val name: String? = null,
    val unique: Boolean = false,
    val sparse: Boolean = false,
    val background: Boolean = true,
    val ttlSeconds: Long? = null,
    val partialFilter: String? = null,
)

data class IndexStat(val name: String, val ops: Long)

fun listIndexes(client: MongoClient, db: String, coll: String): List<IndexInfo> {
    val docs = client.getDatabase(db).getCollection(coll).listIndexes().toList()
    return docs.map { d ->
        @Suppress("UNCHECKED_CAST")
        val keyMap = (d["key"] as Document).toMap()
        IndexInfo(
            name = d.getString("name"),
            keys = keyMap.map { (k, v) -> IndexKey(k, v.toString()) },
            unique = d.getBoolean("unique", false),
            sparse = d.getBoolean("sparse", false),
            background = d.getBoolean("background", false),
            ttlSeconds = (d["expireAfterSeconds"] as? Number)?.toLong(),
            partialFilter = (d["partialFilterExpression"] as? Document)?.toJson(),
        )
    }
}

fun createIndex(client: MongoClient, db: String, coll: String, input: CreateIndexInput): String {
    val spec = Document()
    for (k in input.keys) {
        val v: Any = k.direction.toIntOrNull() ?: k.direction
        spec[k.field] = v
    }
    val opts = IndexOptions()
    input.name?.let { opts.name(it) }
    if (input.unique) opts.unique(true)
    if (input.sparse) opts.sparse(true)
    if (input.background) opts.background(true)
    input.ttlSeconds?.let { opts.expireAfter(it, java.util.concurrent.TimeUnit.SECONDS) }
    input.partialFilter?.takeIf { it.isNotBlank() }?.let {
        opts.partialFilterExpression(Document.parse(it))
    }
    return client.getDatabase(db).getCollection(coll).createIndex(spec, opts)
}

fun dropIndex(client: MongoClient, db: String, coll: String, name: String) {
    client.getDatabase(db).getCollection(coll).dropIndex(name)
}

fun indexStats(client: MongoClient, db: String, coll: String): List<IndexStat> {
    return runCatching {
        client.getDatabase(db).getCollection(coll)
            .aggregate(listOf(Document("\$indexStats", Document())))
            .toList()
            .map {
                IndexStat(
                    name = it.getString("name"),
                    ops = ((it["accesses"] as? Document)?.get("ops") as? Number)?.toLong() ?: 0L,
                )
            }
    }.getOrDefault(emptyList())
}
