package io.mex.migration

import com.mongodb.MongoBulkWriteException
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.BulkWriteOptions
import com.mongodb.client.model.InsertManyOptions
import com.mongodb.client.model.ReplaceOneModel
import com.mongodb.client.model.ReplaceOptions
import io.mex.AppContext
import io.mex.data.ConflictPolicy
import io.mex.data.MigrationCheckpoint
import io.mex.data.MigrationJob
import io.mex.data.MigrationNs
import io.mex.data.MigrationPhase
import io.mex.data.MigrationProgress
import io.mex.data.MigrationStatus
import io.mex.data.NsReport
import io.mex.data.PreflightCheck
import io.mex.data.PreflightResult
import io.mex.data.VerificationReport
import io.mex.mongo.MongoRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import org.bson.Document
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import java.util.concurrent.ConcurrentHashMap

private const val BATCH = 1000
private const val ERROR_SAMPLE_LIMIT = 20

private val DUP_KEY_CODES = setOf(11000, 11001)

/** Canonical EJSON for the checkpoint _id — round-trips every BSON type (MIG-CKPT-1). */
private val CANON: JsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build()

fun encodeLastId(id: Any?): String = Document("_id", id).toJson(CANON)

/**
 * Builds the resume filter from a checkpointed _id. Legacy (pre-v3.1) checkpoint
 * fragments fail to parse and reset the namespace copy from the start (MIG-CKPT-3).
 */
fun resumeFilter(lastIdEjson: String?): Document {
    val id = lastIdEjson?.let { runCatching { Document.parse(it)["_id"] }.getOrNull() }
    return id?.let { Document("_id", Document("\$gt", it)) } ?: Document()
}

data class BatchOutcome(val inserted: Int, val skipped: Int, val errors: Int, val samples: List<String>)

/**
 * Writes one batch under the job's conflict policy (MIG-CONFLICT-2..4, MIG-ERR-1/2).
 * Under [ConflictPolicy.abort] / [ConflictPolicy.drop] a bulk-write error propagates
 * and fails the job by design.
 */
fun writeBatch(tgt: MongoCollection<Document>, batch: List<Document>, policy: ConflictPolicy): BatchOutcome {
    if (batch.isEmpty()) return BatchOutcome(0, 0, 0, emptyList())
    return when (policy) {
        ConflictPolicy.abort, ConflictPolicy.drop -> {
            tgt.insertMany(batch, InsertManyOptions().ordered(false))
            BatchOutcome(batch.size, 0, 0, emptyList())
        }
        ConflictPolicy.append -> try {
            tgt.insertMany(batch, InsertManyOptions().ordered(false))
            BatchOutcome(batch.size, 0, 0, emptyList())
        } catch (e: MongoBulkWriteException) {
            val (dup, other) = e.writeErrors.partition { it.code in DUP_KEY_CODES }
            BatchOutcome(
                inserted = batch.size - e.writeErrors.size,
                skipped = dup.size,
                errors = other.size,
                samples = other.map { it.message }.distinct().take(ERROR_SAMPLE_LIMIT),
            )
        }
        ConflictPolicy.upsert -> {
            val models = batch.map { ReplaceOneModel(Document("_id", it["_id"]), it, ReplaceOptions().upsert(true)) }
            try {
                tgt.bulkWrite(models, BulkWriteOptions().ordered(false))
                BatchOutcome(batch.size, 0, 0, emptyList())
            } catch (e: MongoBulkWriteException) {
                BatchOutcome(
                    inserted = batch.size - e.writeErrors.size,
                    skipped = 0,
                    errors = e.writeErrors.size,
                    samples = e.writeErrors.map { it.message }.distinct().take(ERROR_SAMPLE_LIMIT),
                )
            }
        }
    }
}

data class IndexCopyOutcome(val copied: Int, val failures: List<String>)

/**
 * Recreates every secondary index of a namespace on the target (MIG-IDX-1..4).
 * Index definitions are replayed verbatim through a raw createIndexes command —
 * full option fidelity with only server-managed fields stripped — one command
 * per index so a single failure doesn't sink the rest.
 */
fun copyCollectionIndexes(source: MongoClient, target: MongoClient, ns: MigrationNs): IndexCopyOutcome {
    var copied = 0
    val failures = mutableListOf<String>()
    val src = source.getDatabase(ns.db).getCollection(ns.coll)
    for (idx in src.listIndexes()) {
        val name = idx.getString("name") ?: continue
        if (name == "_id_") continue
        val clean = Document(idx).apply { remove("v"); remove("ns"); remove("background") }
        try {
            target.getDatabase(ns.db).runCommand(
                Document("createIndexes", ns.coll).append("indexes", listOf(clean)),
            )
            copied++
        } catch (e: Exception) {
            failures += "$name (${e.message})"
        }
    }
    return IndexCopyOutcome(copied, failures)
}

/** Count + index verification for one namespace (MIG-VERIFY-2/3). Read-only. */
fun verifyNamespace(
    source: MongoClient,
    target: MongoClient,
    ns: MigrationNs,
    policy: ConflictPolicy,
    skipped: Long,
    errors: Long,
    indexesCopied: Int,
    missingIndexes: List<String>,
    errorSamples: List<String>,
    indexesEnabled: Boolean,
): NsReport {
    val s = source.getDatabase(ns.db).getCollection(ns.coll).countDocuments()
    val t = target.getDatabase(ns.db).getCollection(ns.coll).countDocuments()
    val countOk = when (policy) {
        ConflictPolicy.abort, ConflictPolicy.drop -> t == s
        ConflictPolicy.append -> t + skipped >= s
        ConflictPolicy.upsert -> t >= s
    }
    val ok = countOk && errors == 0L && (!indexesEnabled || missingIndexes.isEmpty())
    return NsReport(ns.db, ns.coll, s, t, skipped, errors, indexesCopied, missingIndexes, errorSamples, ok)
}

class MigrationRunner(
    private val ctx: AppContext,
    private val registry: MongoRegistry,
) {
    private val active = ConcurrentHashMap<String, Boolean>() // jobId -> pauseRequested
    private val cancels = ConcurrentHashMap<String, Boolean>()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    private val _progress = MutableSharedFlow<MigrationProgress>(extraBufferCapacity = 256)
    val progress: SharedFlow<MigrationProgress> = _progress

    fun isRunning(jobId: String): Boolean = active.containsKey(jobId)

    fun pause(jobId: String) { active[jobId] = true }
    fun cancel(jobId: String) { cancels[jobId] = true; active[jobId] = true }

    fun start(jobId: String) {
        val job = ctx.migrations.get(jobId) ?: return
        if (active.containsKey(jobId)) return
        val source = registry.client(job.spec.sourceId) ?: return run {
            ctx.migrations.setStatus(jobId, MigrationStatus.failed, "Source connection not open")
        }
        val target = registry.client(job.spec.targetId) ?: return run {
            ctx.migrations.setStatus(jobId, MigrationStatus.failed, "Target connection not open")
        }
        active[jobId] = false
        ctx.migrations.setStatus(jobId, MigrationStatus.running)
        scope.launch {
            try { runLoop(job, source, target) } finally {
                active.remove(jobId); cancels.remove(jobId)
            }
        }
    }

    private suspend fun runLoop(job: MigrationJob, source: MongoClient, target: MongoClient) {
        val spec = job.spec
        val namespaces = spec.namespaces
        var cp = job.checkpoint ?: MigrationCheckpoint()
        // Report rows accumulate across pause/resume (MIG-VERIFY-4).
        val reportRows = job.report?.rows.orEmpty().toMutableList()
        try {
            while (cp.nsIndex < namespaces.size) {
                val ns = namespaces[cp.nsIndex]
                val freshNs = cp.copiedInNs == 0L && cp.lastIdEjson == null
                if (spec.conflictPolicy == ConflictPolicy.drop && freshNs) {
                    // Drop exactly once per namespace; a resumed checkpoint never re-drops (MIG-CONFLICT-5).
                    runCatching { target.getDatabase(ns.db).getCollection(ns.coll).drop() }
                }
                if (spec.ensureCollections) {
                    runCatching { target.getDatabase(ns.db).createCollection(ns.coll) }
                }
                val src = source.getDatabase(ns.db).getCollection(ns.coll)
                val tgt = target.getDatabase(ns.db).getCollection(ns.coll)
                val estimated = src.estimatedDocumentCount()

                val batch = mutableListOf<Document>()

                suspend fun flush() {
                    if (batch.isEmpty()) return
                    val out = writeBatch(tgt, batch, spec.conflictPolicy)
                    cp = cp.copy(
                        copiedInNs = cp.copiedInNs + out.inserted,
                        skippedInNs = cp.skippedInNs + out.skipped,
                        errorsInNs = cp.errorsInNs + out.errors,
                        lastIdEjson = encodeLastId(batch.last()["_id"]),
                    )
                    batch.clear()
                    if (cp.errorsInNs > spec.docErrorLimit) {
                        throw RuntimeException(
                            "Too many per-document errors in ${ns.db}.${ns.coll} " +
                                "(${cp.errorsInNs} > limit ${spec.docErrorLimit}).",
                        )
                    }
                    ctx.migrations.setCheckpoint(job.id, cp)
                    _progress.emit(
                        MigrationProgress(
                            job.id, MigrationStatus.running, ns, cp.copiedInNs, estimated,
                            phase = MigrationPhase.copy, skipped = cp.skippedInNs, docErrors = cp.errorsInNs,
                        ),
                    )
                }

                val cursor = src.find(resumeFilter(cp.lastIdEjson)).sort(Document("_id", 1))
                for (doc in cursor) {
                    if (cancels[job.id] == true) throw RuntimeException("Cancelled by user.")
                    if (active[job.id] == true) {
                        flush()
                        ctx.migrations.setStatus(job.id, MigrationStatus.paused)
                        _progress.emit(
                            MigrationProgress(
                                job.id, MigrationStatus.paused, ns, cp.copiedInNs, estimated,
                                phase = MigrationPhase.copy, skipped = cp.skippedInNs, docErrors = cp.errorsInNs,
                            ),
                        )
                        return
                    }
                    batch.add(doc)
                    if (batch.size >= BATCH) flush()
                }
                flush()

                // Post-copy phases for this namespace: indexes, then verification.
                var indexesCopied = 0
                var missingIndexes = emptyList<String>()
                if (spec.copyIndexes) {
                    _progress.emit(
                        MigrationProgress(
                            job.id, MigrationStatus.running, ns, cp.copiedInNs, estimated,
                            phase = MigrationPhase.indexes, skipped = cp.skippedInNs, docErrors = cp.errorsInNs,
                        ),
                    )
                    val outcome = copyCollectionIndexes(source, target, ns)
                    indexesCopied = outcome.copied
                    missingIndexes = outcome.failures
                }

                val row = if (spec.verify) {
                    _progress.emit(
                        MigrationProgress(
                            job.id, MigrationStatus.running, ns, cp.copiedInNs, estimated,
                            phase = MigrationPhase.verify, skipped = cp.skippedInNs, docErrors = cp.errorsInNs,
                        ),
                    )
                    runCatching {
                        verifyNamespace(
                            source, target, ns, spec.conflictPolicy,
                            cp.skippedInNs, cp.errorsInNs, indexesCopied, missingIndexes,
                            emptyList(), spec.copyIndexes,
                        )
                    }.getOrElse { e ->
                        NsReport(
                            ns.db, ns.coll, -1, -1, cp.skippedInNs, cp.errorsInNs,
                            indexesCopied, missingIndexes, listOf("verification failed: ${e.message}"), ok = false,
                        )
                    }
                } else {
                    NsReport(
                        ns.db, ns.coll, -1, -1, cp.skippedInNs, cp.errorsInNs,
                        indexesCopied, missingIndexes, emptyList(),
                        ok = cp.errorsInNs == 0L && missingIndexes.isEmpty(),
                    )
                }
                reportRows.removeAll { it.db == ns.db && it.coll == ns.coll }
                reportRows += row
                ctx.migrations.setReport(
                    job.id,
                    VerificationReport(reportRows.all { it.ok }, System.currentTimeMillis(), reportRows.toList()),
                )

                cp = MigrationCheckpoint(nsIndex = cp.nsIndex + 1)
                ctx.migrations.setCheckpoint(job.id, cp)
            }
            ctx.migrations.setStatus(job.id, MigrationStatus.completed)
            _progress.emit(MigrationProgress(job.id, MigrationStatus.completed, null, 0L, null))
        } catch (e: Exception) {
            ctx.migrations.setStatus(job.id, MigrationStatus.failed, e.message)
            _progress.emit(MigrationProgress(job.id, MigrationStatus.failed, null, 0L, null, e.message))
        }
    }
}

/** Preflight — reachability + per-namespace source/target shape (MIG-CONFLICT-6, MIG-UI-2). */
fun preflight(
    source: MongoClient,
    target: MongoClient,
    namespaces: List<MigrationNs>,
    policy: ConflictPolicy = ConflictPolicy.abort,
): PreflightResult {
    val checks = mutableListOf<PreflightCheck>()
    checks += ping("Source reachable", source)
    checks += ping("Target reachable", target)
    for (ns in namespaces) {
        val srcExists = runCatching {
            source.getDatabase(ns.db).listCollectionNames().any { it == ns.coll }
        }.getOrDefault(false)
        if (!srcExists) {
            checks += PreflightCheck("${ns.db}.${ns.coll}", false, "missing on source")
            continue
        }
        val srcCount = runCatching {
            source.getDatabase(ns.db).getCollection(ns.coll).estimatedDocumentCount()
        }.getOrDefault(-1L)
        val tgtExists = runCatching {
            target.getDatabase(ns.db).listCollectionNames().any { it == ns.coll }
        }.getOrDefault(false)
        val tgtCount = if (tgtExists) {
            runCatching { target.getDatabase(ns.db).getCollection(ns.coll).estimatedDocumentCount() }.getOrDefault(-1L)
        } else null
        val tgtLabel = when {
            tgtCount == null -> "not present"
            tgtCount < 0 -> "present"
            else -> "%,d docs".format(tgtCount)
        }
        val srcLabel = if (srcCount < 0) "?" else "~%,d docs".format(srcCount)
        checks += PreflightCheck("${ns.db}.${ns.coll}", true, "$srcLabel → target: $tgtLabel")
        if (policy == ConflictPolicy.abort && (tgtCount ?: 0L) > 0L) {
            checks += PreflightCheck(
                "${ns.db}.${ns.coll} target is non-empty",
                true,
                "policy Abort will fail on the first duplicate _id — consider Append or Upsert",
                warn = true,
            )
        }
    }
    return PreflightResult(checks.all { it.ok }, checks)
}

private fun ping(label: String, client: MongoClient): PreflightCheck = runCatching {
    client.getDatabase("admin").runCommand(Document("ping", 1))
    PreflightCheck(label, true)
}.getOrElse { PreflightCheck(label, false, it.message) }
