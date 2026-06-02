package io.mex.migration

import com.mongodb.client.MongoClient
import io.mex.AppContext
import io.mex.data.MigrationCheckpoint
import io.mex.data.MigrationJob
import io.mex.data.MigrationNs
import io.mex.data.MigrationProgress
import io.mex.data.MigrationStatus
import io.mex.data.PreflightCheck
import io.mex.data.PreflightResult
import io.mex.mongo.MongoRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import org.bson.Document
import java.util.concurrent.ConcurrentHashMap

private const val BATCH = 1000
private const val CHECKPOINT_EVERY = 200

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
        val namespaces = job.spec.namespaces
        var cp = job.checkpoint ?: MigrationCheckpoint()
        try {
            while (cp.nsIndex < namespaces.size) {
                val ns = namespaces[cp.nsIndex]
                if (job.spec.ensureCollections) {
                    runCatching { target.getDatabase(ns.db).createCollection(ns.coll) }
                }
                val src = source.getDatabase(ns.db).getCollection(ns.coll)
                val tgt = target.getDatabase(ns.db).getCollection(ns.coll)
                val estimated = src.estimatedDocumentCount()

                val filter: Document = cp.lastIdEjson?.let {
                    Document("_id", Document("\$gt", Document.parse("""{"_": $it}""")["_"]))
                } ?: Document()

                val batch = mutableListOf<Document>()
                var lastIdJson: String? = null
                val cursor = src.find(filter).sort(Document("_id", 1))
                for (doc in cursor) {
                    if (cancels[job.id] == true) throw RuntimeException("Cancelled by user.")
                    if (active[job.id] == true) {
                        if (batch.isNotEmpty()) tgt.insertMany(batch)
                        val nextCp = cp.copy(copiedInNs = cp.copiedInNs + batch.size, lastIdEjson = lastIdJson ?: cp.lastIdEjson)
                        ctx.migrations.setCheckpoint(job.id, nextCp)
                        ctx.migrations.setStatus(job.id, MigrationStatus.paused)
                        _progress.emit(MigrationProgress(job.id, MigrationStatus.paused, ns, nextCp.copiedInNs, estimated))
                        return
                    }
                    batch.add(doc)
                    val idDoc = Document("_id", doc["_id"])
                    lastIdJson = idDoc.toJson().substringAfter("\"_id\":").substringBefore("}")
                    if (batch.size >= BATCH) {
                        tgt.insertMany(batch)
                        cp = cp.copy(copiedInNs = cp.copiedInNs + batch.size, lastIdEjson = lastIdJson)
                        batch.clear()
                        if (cp.copiedInNs % CHECKPOINT_EVERY == 0L) ctx.migrations.setCheckpoint(job.id, cp)
                        _progress.emit(MigrationProgress(job.id, MigrationStatus.running, ns, cp.copiedInNs, estimated))
                    }
                }
                if (batch.isNotEmpty()) {
                    tgt.insertMany(batch)
                    cp = cp.copy(copiedInNs = cp.copiedInNs + batch.size, lastIdEjson = lastIdJson)
                }
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

fun preflight(source: MongoClient, target: MongoClient, namespaces: List<MigrationNs>): PreflightResult {
    val checks = mutableListOf<PreflightCheck>()
    checks += ping("Source reachable", source)
    checks += ping("Target reachable", target)
    for (ns in namespaces) {
        val ok = runCatching {
            source.getDatabase(ns.db).listCollectionNames().any { it == ns.coll }
        }.getOrDefault(false)
        checks += PreflightCheck("Source has ${ns.db}.${ns.coll}", ok, if (!ok) "missing" else null)
    }
    return PreflightResult(checks.all { it.ok }, checks)
}

private fun ping(label: String, client: MongoClient): PreflightCheck = runCatching {
    client.getDatabase("admin").runCommand(Document("ping", 1))
    PreflightCheck(label, true)
}.getOrElse { PreflightCheck(label, false, it.message) }
