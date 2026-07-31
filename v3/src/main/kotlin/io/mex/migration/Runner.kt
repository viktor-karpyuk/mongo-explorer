package io.mex.migration

import com.mongodb.MongoBulkWriteException
import com.mongodb.ReadPreference
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
import io.mex.data.MigrationSpec
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
import org.bson.RawBsonDocument
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import java.util.concurrent.ConcurrentHashMap

private const val ERROR_SAMPLE_LIMIT = 20

/**
 * Byte ceiling for one bulk write. The wire protocol caps a command at 48 MB and
 * documents can be 16 MB each, so a doc-count-only cap can build a batch the server
 * rejects (and that sits entirely in heap). 12 MB matches mongodump/mongosync.
 */
private const val BATCH_BYTES = 12 * 1024 * 1024

private val DUP_KEY_CODES = setOf(11000, 11001)

/** Server-managed index fields that must not be replayed onto the target. */
private val INDEX_SERVER_FIELDS = listOf("v", "ns", "background")

/** Collection options that cannot be passed back to `create`. */
private val OPTION_SERVER_FIELDS = listOf("uuid", "info", "idIndex")

/** Canonical EJSON for the checkpoint _id — round-trips every BSON type (MIG-CKPT-1). */
private val CANON: JsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build()

fun encodeLastId(id: Any?): String = Document("_id", id).toJson(CANON)

/**
 * Builds the resume filter from a checkpointed _id. Legacy (pre-v3.1) checkpoint
 * fragments fail to parse and reset the namespace copy from the start (MIG-CKPT-3).
 *
 * Uses `$expr` rather than a plain `$gt` because MongoDB query operators are
 * type-bracketed: `{_id: {$gt: ObjectId(…)}}` silently matches *only* ObjectIds, so a
 * collection with mixed `_id` types would drop every document whose type sorts
 * before the checkpoint's. Aggregation comparison spans the BSON type order (MIG-CKPT-5).
 */
fun resumeFilter(lastIdEjson: String?): Document {
    val id = lastIdEjson?.let { runCatching { Document.parse(it)["_id"] }.getOrNull() } ?: return Document()
    return Document("\$expr", Document("\$gt", listOf("\$_id", id)))
}

data class BatchOutcome(val inserted: Int, val skipped: Int, val errors: Int, val samples: List<String>)

/**
 * Writes one batch under the job's conflict policy (MIG-CONFLICT-2..4, MIG-ERR-1/2).
 * Under [ConflictPolicy.abort] / [ConflictPolicy.drop] a bulk-write error propagates
 * and fails the job by design.
 */
fun writeBatch(
    tgt: MongoCollection<RawBsonDocument>,
    batch: List<RawBsonDocument>,
    policy: ConflictPolicy,
): BatchOutcome {
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
            val models = batch.map {
                ReplaceOneModel(Document("_id", it["_id"]), it, ReplaceOptions().upsert(true))
            }
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

/**
 * The policy to use for the first batch of a resumed namespace (MIG-CKPT-4).
 *
 * A checkpoint is written *before* the batch and advanced after, so a crash between the
 * two leaves `pendingLastIdEjson` set — the batch may or may not have landed. Replaying
 * it under `abort`/`drop` would hit a duplicate `_id` and fail the job permanently, so
 * that one batch goes through as an idempotent upsert. `append` (skips duplicates) and
 * `upsert` are already replay-safe and keep their own semantics.
 */
fun replayPolicy(policy: ConflictPolicy): ConflictPolicy = when (policy) {
    ConflictPolicy.abort, ConflictPolicy.drop -> ConflictPolicy.upsert
    ConflictPolicy.append, ConflictPolicy.upsert -> policy
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
        val clean = Document(idx).apply { INDEX_SERVER_FIELDS.forEach { remove(it) } }
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

/** Source-side shape of a namespace, read once from `listCollections`. */
data class NsShape(val exists: Boolean, val type: String, val options: Document)

fun readNsShape(client: MongoClient, ns: MigrationNs): NsShape {
    val doc = runCatching {
        client.getDatabase(ns.db)
            .listCollections()
            .filter(Document("name", ns.coll))
            .firstOrNull()
    }.getOrNull() ?: return NsShape(false, "collection", Document())
    val options = (doc["options"] as? Document) ?: Document()
    val type = doc.getString("type") ?: "collection"
    return NsShape(true, type, options)
}

/**
 * Creates the target collection carrying the source's own options (MIG-OPT-1).
 * A bare `createCollection` silently drops validators, collation, capped-ness and
 * time-series config — a time-series source would materialise as a plain collection.
 * Replaying the raw options document through `create` preserves all of them.
 */
fun ensureCollection(target: MongoClient, ns: MigrationNs, options: Document?) {
    val clean = Document(options ?: Document()).apply { OPTION_SERVER_FIELDS.forEach { remove(it) } }
    val cmd = Document("create", ns.coll).apply { clean.forEach { (k, v) -> append(k, v) } }
    runCatching { target.getDatabase(ns.db).runCommand(cmd) }
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

/** Raised when the user cancels, so the job records `cancelled` rather than `failed`. */
private class CancelledException : RuntimeException("Cancelled by user.")

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

    /** Clears the checkpoint so a resumable job starts over from the first namespace. */
    fun restart(jobId: String) {
        ctx.migrations.setCheckpoint(jobId, MigrationCheckpoint())
        ctx.migrations.clearReport(jobId)
        start(jobId)
    }

    fun start(jobId: String) {
        val job = ctx.migrations.get(jobId) ?: return
        if (active.containsKey(jobId)) return
        // The picker already hides read-only targets, but a job created before the flag
        // was set (or flipped later) must still refuse to write (DBA-RO-1).
        if (ctx.connections.isReadOnly(job.spec.targetId)) {
            ctx.migrations.setStatus(jobId, MigrationStatus.failed, "Target connection is marked read-only")
            return
        }
        val source = registry.client(job.spec.sourceId) ?: return run {
            ctx.migrations.setStatus(jobId, MigrationStatus.failed, "Source connection not open")
        }
        val target = registry.client(job.spec.targetId) ?: return run {
            ctx.migrations.setStatus(jobId, MigrationStatus.failed, "Target connection not open")
        }
        active[jobId] = false
        ctx.migrations.setStatus(jobId, MigrationStatus.running, clearError = true)
        scope.launch {
            try { runLoop(job, source, target) } finally {
                active.remove(jobId); cancels.remove(jobId)
            }
        }
    }

    private fun sourceCollection(client: MongoClient, ns: MigrationNs, spec: MigrationSpec) =
        client.getDatabase(ns.db)
            .getCollection(ns.coll, RawBsonDocument::class.java)
            .let { if (spec.readFromSecondary) it.withReadPreference(ReadPreference.secondaryPreferred()) else it }

    private suspend fun runLoop(job: MigrationJob, source: MongoClient, target: MongoClient) {
        val spec = job.spec
        val namespaces = spec.namespaces
        var cp = job.checkpoint ?: MigrationCheckpoint()
        // Report rows accumulate across pause/resume (MIG-VERIFY-4).
        val reportRows = job.report?.rows.orEmpty().toMutableList()
        val startedAt = System.currentTimeMillis()
        var copiedThisRun = 0L
        try {
            while (cp.nsIndex < namespaces.size) {
                val ns = namespaces[cp.nsIndex]
                val freshNs = cp.copiedInNs == 0L && cp.lastIdEjson == null
                if (spec.conflictPolicy == ConflictPolicy.drop && freshNs) {
                    // Drop exactly once per namespace; a resumed checkpoint never re-drops (MIG-CONFLICT-5).
                    runCatching { target.getDatabase(ns.db).getCollection(ns.coll).drop() }
                }
                if (spec.ensureCollections) {
                    val shape = if (spec.copyCollectionOptions) readNsShape(source, ns) else null
                    ensureCollection(target, ns, shape?.options)
                }
                val src = sourceCollection(source, ns, spec)
                val tgt = target.getDatabase(ns.db).getCollection(ns.coll, RawBsonDocument::class.java)
                val estimated = runCatching { src.estimatedDocumentCount() }.getOrDefault(0L)

                val batch = mutableListOf<RawBsonDocument>()
                var batchBytes = 0
                // A checkpoint left mid-write means the last batch may or may not have landed.
                var replayWindow = cp.pendingLastIdEjson != null
                val samples = cp.errorSamples.toMutableList()

                suspend fun flush() {
                    if (batch.isEmpty()) return
                    val lastId = encodeLastId(batch.last()["_id"])
                    // Phase 1 — record intent, so a crash mid-write is detectable on resume.
                    cp = cp.copy(pendingLastIdEjson = lastId)
                    ctx.migrations.setCheckpoint(job.id, cp)

                    val policy = if (replayWindow) replayPolicy(spec.conflictPolicy) else spec.conflictPolicy
                    replayWindow = false
                    val out = writeBatch(tgt, batch, policy)
                    for (s in out.samples) if (samples.size < ERROR_SAMPLE_LIMIT && s !in samples) samples += s

                    // Phase 2 — advance the checkpoint and clear the intent marker.
                    cp = cp.copy(
                        copiedInNs = cp.copiedInNs + out.inserted,
                        skippedInNs = cp.skippedInNs + out.skipped,
                        errorsInNs = cp.errorsInNs + out.errors,
                        lastIdEjson = lastId,
                        pendingLastIdEjson = null,
                        errorSamples = samples.toList(),
                    )
                    copiedThisRun += out.inserted
                    batch.clear()
                    batchBytes = 0
                    if (cp.errorsInNs > spec.docErrorLimit) {
                        ctx.migrations.setCheckpoint(job.id, cp)
                        throw RuntimeException(
                            "Too many per-document errors in ${ns.db}.${ns.coll} " +
                                "(${cp.errorsInNs} > limit ${spec.docErrorLimit}).",
                        )
                    }
                    ctx.migrations.setCheckpoint(job.id, cp)
                    _progress.emit(
                        progressOf(job.id, MigrationStatus.running, ns, cp, estimated, MigrationPhase.copy,
                            startedAt, copiedThisRun, namespaces.size),
                    )
                }

                val docLimit = spec.batchSize.coerceIn(1, 100_000)
                val cursor = src.find(resumeFilter(cp.lastIdEjson)).sort(Document("_id", 1))
                for (doc in cursor) {
                    if (cancels[job.id] == true) throw CancelledException()
                    if (active[job.id] == true) {
                        flush()
                        ctx.migrations.setStatus(job.id, MigrationStatus.paused)
                        _progress.emit(
                            progressOf(job.id, MigrationStatus.paused, ns, cp, estimated, MigrationPhase.copy,
                                startedAt, copiedThisRun, namespaces.size),
                        )
                        return
                    }
                    batch.add(doc)
                    batchBytes += doc.byteBuffer.remaining()
                    if (batch.size >= docLimit || batchBytes >= BATCH_BYTES) flush()
                }
                flush()

                // Post-copy phases for this namespace: indexes, then verification.
                var indexesCopied = 0
                var missingIndexes = emptyList<String>()
                if (spec.copyIndexes) {
                    _progress.emit(
                        progressOf(job.id, MigrationStatus.running, ns, cp, estimated, MigrationPhase.indexes,
                            startedAt, copiedThisRun, namespaces.size),
                    )
                    val outcome = copyCollectionIndexes(source, target, ns)
                    indexesCopied = outcome.copied
                    missingIndexes = outcome.failures
                }

                val row = if (spec.verify) {
                    _progress.emit(
                        progressOf(job.id, MigrationStatus.running, ns, cp, estimated, MigrationPhase.verify,
                            startedAt, copiedThisRun, namespaces.size),
                    )
                    runCatching {
                        verifyNamespace(
                            source, target, ns, spec.conflictPolicy,
                            cp.skippedInNs, cp.errorsInNs, indexesCopied, missingIndexes,
                            samples.toList(), spec.copyIndexes,
                        )
                    }.getOrElse { e ->
                        NsReport(
                            ns.db, ns.coll, -1, -1, cp.skippedInNs, cp.errorsInNs,
                            indexesCopied, missingIndexes,
                            samples + "verification failed: ${e.message}", ok = false,
                        )
                    }
                } else {
                    NsReport(
                        ns.db, ns.coll, -1, -1, cp.skippedInNs, cp.errorsInNs,
                        indexesCopied, missingIndexes, samples.toList(),
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
        } catch (e: CancelledException) {
            ctx.migrations.setStatus(job.id, MigrationStatus.cancelled, e.message)
            _progress.emit(MigrationProgress(job.id, MigrationStatus.cancelled, null, 0L, null, e.message))
        } catch (e: Exception) {
            ctx.migrations.setStatus(job.id, MigrationStatus.failed, e.message)
            _progress.emit(MigrationProgress(job.id, MigrationStatus.failed, null, 0L, null, e.message))
        }
    }

    private fun progressOf(
        jobId: String,
        status: MigrationStatus,
        ns: MigrationNs,
        cp: MigrationCheckpoint,
        estimated: Long,
        phase: MigrationPhase,
        startedAt: Long,
        copiedThisRun: Long,
        nsTotal: Int,
    ): MigrationProgress {
        val elapsed = (System.currentTimeMillis() - startedAt) / 1000.0
        return MigrationProgress(
            jobId = jobId,
            status = status,
            currentNs = ns,
            copied = cp.copiedInNs,
            estimated = estimated,
            phase = phase,
            skipped = cp.skippedInNs,
            docErrors = cp.errorsInNs,
            docsPerSecond = if (elapsed > 0.5 && copiedThisRun > 0) copiedThisRun / elapsed else null,
            nsIndex = cp.nsIndex,
            nsTotal = nsTotal,
        )
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
    if (namespaces.isNotEmpty()) {
        checks += PreflightCheck(
            "Source should be quiescent",
            true,
            "the copy walks a live cursor — writes during the run may be missed or captured mid-update",
            warn = true,
        )
    }
    for (ns in namespaces) {
        val shape = readNsShape(source, ns)
        if (!shape.exists) {
            checks += PreflightCheck("${ns.db}.${ns.coll}", false, "missing on source")
            continue
        }
        if (shape.type == "view") {
            checks += PreflightCheck(
                "${ns.db}.${ns.coll} is a view",
                false,
                "views cannot be migrated as data — copy the underlying collection instead",
            )
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
        if (shape.options.containsKey("timeseries")) {
            checks += PreflightCheck(
                "${ns.db}.${ns.coll} is a time-series collection",
                true,
                "target is created with matching timeseries options; keep \"copy collection options\" enabled",
                warn = true,
            )
        }
        if (shape.options.containsKey("validator")) {
            checks += PreflightCheck(
                "${ns.db}.${ns.coll} has a schema validator",
                true,
                "replayed onto the target; documents violating it will be rejected per-document",
                warn = true,
            )
        }
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
