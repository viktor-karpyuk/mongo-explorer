package io.mex.mongo

import com.mongodb.client.MongoClient
import org.bson.Document

/* ============================ models ============================ */

/** Storage footprint of one collection (DBA-STG-1). */
data class CollWeight(
    val db: String,
    val coll: String,
    val count: Long,
    val avgObjSize: Double,
    val dataSize: Long,
    val storageSize: Long,
    /** WiredTiger's "file bytes available for reuse" — the honest reclaimable number. */
    val freeStorageSize: Long?,
    val indexCount: Int,
    val indexSize: Long,
    val indexSizes: Map<String, Long>,
) {
    val ns: String get() = "$db.$coll"
    val totalSize: Long get() = storageSize + indexSize
    val reclaimablePct: Double get() = fragmentationPct(dataSize, storageSize, freeStorageSize)
}

/** One index joined across `$indexStats` (usage) and `listIndexes` (definition). */
data class IndexUsage(
    val db: String,
    val coll: String,
    val name: String,
    val ops: Long,
    val sinceMillis: Long?,
    val sizeBytes: Long,
    val unique: Boolean,
    val ttlSeconds: Long?,
) {
    val ns: String get() = "$db.$coll"
    val id: String get() = "$ns.$name"
}

data class StorageReport(
    val collections: List<CollWeight>,
    val indexes: List<IndexUsage>,
    val errors: List<String>,
) {
    val totalData: Long get() = collections.sumOf { it.dataSize }
    val totalStorage: Long get() = collections.sumOf { it.storageSize }
    val totalIndex: Long get() = collections.sumOf { it.indexSize }
}

/* ===================== pure logic (tested) ===================== */

/**
 * Percentage of the collection's file that is not live data. Prefers WiredTiger's
 * `freeStorageSize` (exact bytes available for reuse); falls back to `1 − data/storage`,
 * which overstates on compressed collections but still ranks compaction candidates.
 */
fun fragmentationPct(dataSize: Long, storageSize: Long, freeStorageSize: Long?): Double = when {
    storageSize <= 0 -> 0.0
    freeStorageSize != null -> (freeStorageSize * 100.0 / storageSize).coerceIn(0.0, 100.0)
    else -> ((1.0 - dataSize.toDouble() / storageSize) * 100.0).coerceIn(0.0, 100.0)
}

/**
 * Indexes with zero recorded reads, ranked by wasted bytes (DBA-STG-2). `_id_` is never
 * a candidate, and TTL indexes are excluded — zero ops is their normal state (the TTL
 * monitor's deletes don't count as accesses) yet they are doing a job.
 */
fun unusedCandidates(indexes: List<IndexUsage>): List<IndexUsage> =
    indexes
        .filter { it.ops == 0L && it.name != "_id_" && it.ttlSeconds == null }
        .sortedByDescending { it.sizeBytes }

/** `$indexStats` counters reset on restart — under a week of observation proves little. */
fun observationDays(sinceMillis: Long?, now: Long = System.currentTimeMillis()): Long? =
    sinceMillis?.let { ((now - it) / 86_400_000L).coerceAtLeast(0) }

/* ============================ scan ============================ */

/** Databases that hold server internals, not user data. */
private val INTERNAL_DBS = setOf("local", "config")

/**
 * Walks every user collection collecting `collStats` + `$indexStats` (DBA-STG-1/2).
 * One command pair per collection — [onProgress] keeps the UI honest on big estates.
 */
fun scanStorage(
    client: MongoClient,
    onProgress: (done: Int, total: Int, ns: String) -> Unit = { _, _, _ -> },
): StorageReport {
    val collections = mutableListOf<CollWeight>()
    val indexes = mutableListOf<IndexUsage>()
    val errors = mutableListOf<String>()

    val targets = mutableListOf<Pair<String, String>>()
    for (db in listDatabases(client).map { it.name }.filter { it !in INTERNAL_DBS }) {
        runCatching {
            listCollections(client, db)
                .filter { it.type == CollectionType.collection }
                .forEach { targets += db to it.name }
        }.onFailure { errors += "$db: ${it.message}" }
    }

    targets.forEachIndexed { i, (db, coll) ->
        onProgress(i, targets.size, "$db.$coll")
        try {
            val s = client.getDatabase(db).runCommand(Document("collStats", coll))
            @Suppress("UNCHECKED_CAST")
            val idxSizes = (s["indexSizes"] as? Document)?.entries
                ?.associate { it.key to ((it.value as? Number)?.toLong() ?: 0L) }
                .orEmpty()
            val weight = CollWeight(
                db = db,
                coll = coll,
                count = (s["count"] as? Number)?.toLong() ?: 0L,
                avgObjSize = (s["avgObjSize"] as? Number)?.toDouble() ?: 0.0,
                dataSize = (s["size"] as? Number)?.toLong() ?: 0L,
                storageSize = (s["storageSize"] as? Number)?.toLong() ?: 0L,
                freeStorageSize = (s["freeStorageSize"] as? Number)?.toLong(),
                indexCount = (s["nindexes"] as? Number)?.toInt() ?: 0,
                indexSize = (s["totalIndexSize"] as? Number)?.toLong() ?: 0L,
                indexSizes = idxSizes,
            )
            collections += weight

            val defs = runCatching { listIndexes(client, db, coll) }.getOrDefault(emptyList())
                .associateBy { it.name }
            client.getDatabase(db).getCollection(coll)
                .aggregate(listOf(Document("\$indexStats", Document())))
                .forEach { stat ->
                    val name = stat.getString("name") ?: return@forEach
                    val accesses = stat["accesses"] as? Document
                    indexes += IndexUsage(
                        db = db,
                        coll = coll,
                        name = name,
                        ops = (accesses?.get("ops") as? Number)?.toLong() ?: 0L,
                        sinceMillis = (accesses?.get("since") as? java.util.Date)?.time,
                        sizeBytes = weight.indexSizes[name] ?: 0L,
                        unique = defs[name]?.unique ?: false,
                        ttlSeconds = defs[name]?.ttlSeconds,
                    )
                }
        } catch (e: Exception) {
            errors += "$db.$coll: ${e.message}"
        }
    }
    onProgress(targets.size, targets.size, "")
    return StorageReport(
        collections = collections.sortedByDescending { it.totalSize },
        indexes = indexes,
        errors = errors,
    )
}
