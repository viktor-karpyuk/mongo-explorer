package io.mex.mongo

import com.mongodb.client.MongoClient

/* ============================ models ============================ */

enum class DiffSide { onlyLeft, onlyRight, both }

/** One collection's presence and document count across the two sides (DBA-DIFF-1). */
data class CollectionDiff(
    val db: String,
    val coll: String,
    val side: DiffSide,
    val leftCount: Long?,
    val rightCount: Long?,
) {
    val countMatches: Boolean get() = side == DiffSide.both && leftCount == rightCount
}

/** One field's presence and dominant type across the two sides (DBA-DIFF-2). */
data class FieldDiff(
    val path: String,
    val side: DiffSide,
    val leftType: String?,
    val rightType: String?,
    val leftPresence: Double?,
    val rightPresence: Double?,
) {
    /** Present on both but the dominant BSON type differs — a real schema drift signal. */
    val typeMismatch: Boolean get() = side == DiffSide.both && leftType != null && rightType != null && leftType != rightType
}

/** One index's presence across the two sides, keyed by the index name (DBA-DIFF-3). */
data class IndexDiff(val name: String, val side: DiffSide, val leftKeys: String?, val rightKeys: String?)

data class NamespaceComparison(
    val fields: List<FieldDiff>,
    val indexes: List<IndexDiff>,
    val leftCount: Long,
    val rightCount: Long,
) {
    val fieldsInSync: Boolean get() = fields.none { it.side != DiffSide.both || it.typeMismatch }
    val indexesInSync: Boolean get() = indexes.all { it.side == DiffSide.both }
    val countsMatch: Boolean get() = leftCount == rightCount
}

/* ===================== pure diff logic (tested) ===================== */

/** Merges two collection listings into one presence/count diff, sorted by namespace. */
fun diffCollections(
    left: Map<String, Long>,  // "db.coll" -> count
    right: Map<String, Long>,
): List<CollectionDiff> {
    return (left.keys + right.keys).toSortedSet().map { ns ->
        val (db, coll) = ns.split(".", limit = 2).let { it[0] to it.getOrElse(1) { "" } }
        val l = left[ns]
        val r = right[ns]
        val side = when {
            l != null && r != null -> DiffSide.both
            l != null -> DiffSide.onlyLeft
            else -> DiffSide.onlyRight
        }
        CollectionDiff(db, coll, side, l, r)
    }
}

/** Merges two schema reports into a per-field diff, tuned-first then alphabetical. */
fun diffFields(left: List<SchemaField>, right: List<SchemaField>): List<FieldDiff> {
    val l = left.associateBy { it.path }
    val r = right.associateBy { it.path }
    return (l.keys + r.keys).toSortedSet().map { path ->
        val lf = l[path]
        val rf = r[path]
        val side = when {
            lf != null && rf != null -> DiffSide.both
            lf != null -> DiffSide.onlyLeft
            else -> DiffSide.onlyRight
        }
        FieldDiff(path, side, lf?.dominantType, rf?.dominantType, lf?.presence, rf?.presence)
    }.sortedWith(
        // Surface differences first: missing fields and type mismatches above the in-sync bulk.
        compareByDescending<FieldDiff> { it.side != DiffSide.both || it.typeMismatch }.thenBy { it.path },
    )
}

fun diffIndexes(left: List<IndexInfo>, right: List<IndexInfo>): List<IndexDiff> {
    fun keyStr(i: IndexInfo) = i.keys.joinToString(", ") { "${it.field}: ${it.direction}" }
    val l = left.associateBy { it.name }
    val r = right.associateBy { it.name }
    return (l.keys + r.keys).toSortedSet().map { name ->
        val li = l[name]
        val ri = r[name]
        val side = when {
            li != null && ri != null -> DiffSide.both
            li != null -> DiffSide.onlyLeft
            else -> DiffSide.onlyRight
        }
        IndexDiff(name, side, li?.let(::keyStr), ri?.let(::keyStr))
    }.sortedWith(compareByDescending<IndexDiff> { it.side != DiffSide.both }.thenBy { it.name })
}

/* ============================ collectors ============================ */

/** Collection name → document count for one connection, excluding server-internal dbs. */
fun collectCollectionCounts(client: MongoClient): Map<String, Long> {
    val out = linkedMapOf<String, Long>()
    for (db in listDatabases(client).map { it.name }.filter { it != "local" && it != "config" && it != "admin" }) {
        runCatching {
            listCollections(client, db)
                .filter { it.type == CollectionType.collection }
                .forEach { c ->
                    out["$db.${c.name}"] = runCatching {
                        client.getDatabase(db).getCollection(c.name).estimatedDocumentCount()
                    }.getOrDefault(-1L)
                }
        }
    }
    return out
}

/** Full namespace comparison: schema (sampled), indexes and exact counts. */
fun compareNamespace(
    left: MongoClient,
    right: MongoClient,
    db: String,
    coll: String,
    sampleSize: Int = 400,
): NamespaceComparison {
    val leftSchema = analyzeSchema(left, db, coll, sampleSize).fields
    val rightSchema = analyzeSchema(right, db, coll, sampleSize).fields
    val leftIdx = runCatching { listIndexes(left, db, coll) }.getOrDefault(emptyList())
    val rightIdx = runCatching { listIndexes(right, db, coll) }.getOrDefault(emptyList())
    val leftCount = runCatching { left.getDatabase(db).getCollection(coll).countDocuments() }.getOrDefault(-1L)
    val rightCount = runCatching { right.getDatabase(db).getCollection(coll).countDocuments() }.getOrDefault(-1L)
    return NamespaceComparison(
        fields = diffFields(leftSchema, rightSchema),
        indexes = diffIndexes(leftIdx, rightIdx),
        leftCount = leftCount,
        rightCount = rightCount,
    )
}
