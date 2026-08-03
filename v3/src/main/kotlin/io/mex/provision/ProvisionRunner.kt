package io.mex.provision

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import io.mex.AppContext
import io.mex.backup.ToolProcess
import io.mex.data.ConnectionInput
import io.mex.data.Lab
import io.mex.data.LabStatus
import io.mex.data.LabTopology
import io.mex.data.ProvisionPhase
import io.mex.mongo.MongoRegistry
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import org.bson.Document
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermission
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.coroutines.resume

sealed class LabEvent {
    abstract val labId: String

    data class Phase(override val labId: String, val phase: ProvisionPhase) : LabEvent()
    data class Log(override val labId: String, val line: String) : LabEvent()

    /** [password]/[uri] are only set on a successful provision — the show-once banner (PRV-SEC-3). */
    data class Done(
        override val labId: String,
        val status: LabStatus,
        val error: String? = null,
        val password: String? = null,
        val uri: String? = null,
    ) : LabEvent()
}

/**
 * Runs every lab mutation (PRV-LIFE-6): the phased provision pipeline (PRV-BOOT-1),
 * stop/start/destroy, and Docker-truth reconciliation (PRV-LIFE-1/4). One operation
 * per lab at a time; all work on the IO dispatcher; progress via [events].
 */
class ProvisionRunner(private val ctx: AppContext, private val registry: MongoRegistry) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val ops = ConcurrentHashMap<String, Job>()
    private val secrets = ConcurrentHashMap<String, String>()

    private val _events = MutableSharedFlow<LabEvent>(extraBufferCapacity = 1024)
    val events: SharedFlow<LabEvent> = _events

    fun isBusy(labId: String): Boolean = ops.containsKey(labId)

    fun cancel(labId: String) {
        ops[labId]?.cancel()
    }

    /** Creates the catalog row and launches the pipeline; returns the lab id. */
    fun provision(name: String, topology: LabTopology, mongoTag: String, auth: Boolean): String {
        val lab = ctx.labs.create(name, topology, mongoTag, auth, ctx.labsDir.toString())
        launchOp(lab.id, opName = "provision") { runProvision(ctx.labs.get(lab.id)!!) }
        return lab.id
    }

    fun start(labId: String) = launchOp(labId, "start") { runStart(ctx.labs.get(labId) ?: return@launchOp) }

    fun stop(labId: String) = launchOp(labId, "stop") { runStop(ctx.labs.get(labId) ?: return@launchOp) }

    fun destroy(labId: String, alsoConnection: Boolean) =
        launchOp(labId, "destroy") { runDestroy(ctx.labs.get(labId) ?: return@launchOp, alsoConnection) }

    /** Docker is the source of truth; corrects every idle lab's row (PRV-LIFE-1/4). */
    fun reconcile() {
        scope.launch {
            val docker = findDocker() ?: return@launch
            val (code, lines) = capture(docker.path, psArgs()) ?: return@launch
            if (code != 0) return@launch
            val byLab = lines.mapNotNull(::parsePsLine).filter { it.labId != null }.groupBy { it.labId!! }
            for (lab in ctx.labs.list()) {
                if (isBusy(lab.id)) continue
                val states = byLab[lab.id].orEmpty().map { it.state }
                val corrected = reconcileStatus(lab.status, states) ?: continue
                val error = when (corrected) {
                    LabStatus.missing -> "containers no longer exist"
                    LabStatus.failed -> "interrupted — the app closed during provisioning"
                    else -> null
                }
                ctx.labs.setStatus(lab.id, corrected, error)
                _events.tryEmit(LabEvent.Done(lab.id, corrected, error))
            }
        }
    }

    /* ===================== operations ===================== */

    private fun launchOp(labId: String, opName: String, body: suspend () -> Unit) {
        if (ops.containsKey(labId)) return
        val job = scope.launch {
            try {
                body()
            } catch (e: CancellationException) {
                ctx.labs.setStatus(labId, LabStatus.failed, "cancelled during $opName")
                _events.tryEmit(LabEvent.Done(labId, LabStatus.failed, "cancelled during $opName"))
                throw e
            } catch (e: Exception) {
                ctx.labs.setStatus(labId, LabStatus.failed, e.message)
                _events.tryEmit(LabEvent.Done(labId, LabStatus.failed, e.message))
            }
        }
        ops[labId] = job
        job.invokeOnCompletion {
            ops.remove(labId)
            secrets.remove(labId)
        }
    }

    private class PhaseError(phase: ProvisionPhase, message: String) :
        RuntimeException("$phase: $message")

    private suspend fun runProvision(lab: Lab) {
        val docker = requireDocker()
        var phase = ProvisionPhase.render
        try {
            /* render — managed dir, ports, keyfile, compose.yaml (PRV-RENDER-1..4) */
            emitPhase(lab.id, ProvisionPhase.render)
            val p = plan(lab)
            val ports = allocatePorts(p.clientServices)
            ctx.labs.setPorts(lab.id, ports)
            val dir = Path.of(lab.dir)
            Files.createDirectories(dir)
            val password = if (lab.auth) generatePassword().also { secrets[lab.id] = it } else null
            if (needsKeyfile(lab)) writeOwnerOnly(dir.resolve("keyfile"), generateKeyfile())
            Files.writeString(dir.resolve("compose.yaml"), renderCompose(lab, ports))

            /* up */
            phase = ProvisionPhase.up
            emitPhase(lab.id, ProvisionPhase.up)
            if (runDocker(lab.id, docker, composeArgs(lab.dir, "up", "-d")) != 0) {
                throw PhaseError(phase, "docker compose up failed — see log")
            }

            /* wait — every mongod healthy; mongos needs an initiated CSRS first (PRV-BOOT-2) */
            phase = ProvisionPhase.wait
            emitPhase(lab.id, ProvisionPhase.wait)
            awaitHealthy(lab, docker, p.mongods.map { it.name })

            /* initiate — CSRS first (plan order), then data-bearing sets (PRV-BOOT-3) */
            phase = ProvisionPhase.initiate
            emitPhase(lab.id, ProvisionPhase.initiate)
            for (rs in p.replicaSets) {
                val container = containerName(p.project, rs.members.first())
                val out = mongosh(lab.id, docker, container, initiateScript(rs.name, rs.members, rs.configSvr))
                if ("PRIMARY" !in out) throw PhaseError(phase, "${rs.name}: no primary elected — see log")
            }

            /* shards — mongos can only get healthy after the CSRS has a primary (PRV-BOOT-4) */
            if (lab.topology is LabTopology.Sharded) {
                phase = ProvisionPhase.shards
                emitPhase(lab.id, ProvisionPhase.shards)
                awaitHealthy(lab, docker, p.mongosList)
                val mongos = containerName(p.project, p.mongosList.first())
                for (rs in p.replicaSets.filterNot { it.configSvr }) {
                    val out = mongosh(lab.id, docker, mongos, addShardScript(rs.name, rs.members))
                    if ("ADDED" !in out) throw PhaseError(phase, "addShard ${rs.name} failed — see log")
                }
                // The shard count is asserted host-side in `verify` (as root): the localhost
                // exception authorizes addShard but not reading config.shards.
            }

            /* auth — root user via localhost exception (PRV-BOOT-5) */
            if (lab.auth && password != null) {
                phase = ProvisionPhase.auth
                emitPhase(lab.id, ProvisionPhase.auth)
                val hosts = when (lab.topology) {
                    is LabTopology.Sharded -> p.mongosList
                    else -> p.mongods.map { it.name }
                }
                createRootWalking(lab.id, docker, p.project, hosts, password)
            }

            /* verify — from the host, through the URI that will be registered (PRV-BOOT-6) */
            phase = ProvisionPhase.verify
            emitPhase(lab.id, ProvisionPhase.verify)
            val uri = labUri(lab, ports, password)
            verifyLab(lab, uri)?.let { throw PhaseError(phase, it) }

            /* register (PRV-CONN-1/3) */
            phase = ProvisionPhase.register
            emitPhase(lab.id, ProvisionPhase.register)
            val connection = ctx.connections.create(
                ConnectionInput(
                    name = "${lab.name} (lab)",
                    uri = uri,
                    notes = "Provisioned by Mongo Explorer — lab ${lab.id}. Dev/test only.",
                ),
            )
            ctx.labs.setConnection(lab.id, connection.id)
            ctx.labs.setStatus(lab.id, LabStatus.running)
            _events.tryEmit(LabEvent.Done(lab.id, LabStatus.running, password = password, uri = uri))
        } catch (e: PhaseError) {
            // Containers stay up for diagnosis; Destroy is offered, never forced (PRV-BOOT-7).
            ctx.labs.setStatus(lab.id, LabStatus.failed, e.message)
            _events.tryEmit(LabEvent.Done(lab.id, LabStatus.failed, e.message))
        }
    }

    private suspend fun runStart(lab: Lab) {
        val docker = requireDocker()
        if (lab.appMajor > io.mex.data.LAB_APP_MAJOR) {
            throw RuntimeException("lab was created by a newer app version — Start is disabled (PRV-LIFE-5)")
        }
        if (runDocker(lab.id, docker, composeArgs(lab.dir, "start")) != 0) {
            throw RuntimeException("docker compose start failed — a lab port may be taken; see log")
        }
        val p = plan(lab)
        awaitHealthy(lab, docker, p.mongods.map { it.name } + p.mongosList)
        // Credentials live in the registered connection; verify reachability only.
        val uri = lab.connectionId?.let { ctx.connections.get(it)?.uri }
        uri?.let { u -> verifyPing(u)?.let { throw RuntimeException("verify: $it") } }
        ctx.labs.setStatus(lab.id, LabStatus.running)
        _events.tryEmit(LabEvent.Done(lab.id, LabStatus.running))
    }

    private suspend fun runStop(lab: Lab) {
        val docker = requireDocker()
        if (runDocker(lab.id, docker, composeArgs(lab.dir, "stop")) != 0) {
            throw RuntimeException("docker compose stop failed — see log")
        }
        // Stop never touches volumes; there is no keep-data flag (PRV-LIFE-2).
        ctx.labs.setStatus(lab.id, LabStatus.stopped)
        _events.tryEmit(LabEvent.Done(lab.id, LabStatus.stopped))
    }

    private suspend fun runDestroy(lab: Lab, alsoConnection: Boolean) {
        val docker = findDocker()
        if (docker != null) {
            // Best-effort: destroy must succeed even when containers are already gone (PRV-LIFE-4).
            runDocker(lab.id, docker, composeArgs(lab.dir, "down", "-v"))
        } else {
            _events.tryEmit(LabEvent.Log(lab.id, "docker unavailable — removing records only"))
        }
        val dir = File(lab.dir)
        // Only ever delete inside the managed labs directory — never a user path (PRV-LIFE-3).
        if (dir.toPath().startsWith(ctx.labsDir)) dir.deleteRecursively()
        if (alsoConnection) {
            lab.connectionId?.let { id ->
                registry.disconnect(id)
                ctx.connections.delete(id)
            }
        }
        ctx.labs.delete(lab.id)
        _events.tryEmit(LabEvent.Done(lab.id, LabStatus.missing))
    }

    /* ===================== helpers ===================== */

    private fun requireDocker() =
        findDocker() ?: throw RuntimeException("docker not found on PATH or common locations")

    private fun emitPhase(labId: String, phase: ProvisionPhase) {
        _events.tryEmit(LabEvent.Phase(labId, phase))
    }

    /** Streams output into the event log (redacted) and resumes with the exit code. */
    private suspend fun runDocker(
        labId: String,
        docker: io.mex.backup.ToolInfo,
        args: List<String>,
        sink: ((String) -> Unit)? = null,
    ): Int = suspendCancellableCoroutine { cont ->
        val secretList = listOfNotNull(secrets[labId])
        val proc = ToolProcess(
            binary = docker.path,
            args = args,
            scope = scope,
            onLine = { line ->
                sink?.invoke(line)
                _events.tryEmit(LabEvent.Log(labId, redact(line, secretList)))
            },
            onExit = { code -> if (cont.isActive) cont.resume(code) },
        )
        cont.invokeOnCancellation { proc.cancel() }
        proc.start()
    }

    /** Runs a mongosh script in a container and returns its full output for sentinel checks. */
    private suspend fun mongosh(
        labId: String,
        docker: io.mex.backup.ToolInfo,
        container: String,
        script: String,
    ): String {
        val out = StringBuilder()
        runDocker(labId, docker, execMongoshArgs(container, script)) { out.appendLine(it) }
        return out.toString()
    }

    /** Polls container health until every service is healthy (PRV-BOOT-2, PRV-NFR-1 budget). */
    private suspend fun awaitHealthy(lab: Lab, docker: io.mex.backup.ToolInfo, services: List<String>) {
        val project = plan(lab).project
        val deadline = System.currentTimeMillis() + 30_000L + 15_000L * services.size
        val pending = services.toMutableSet()
        while (pending.isNotEmpty()) {
            for (svc in pending.toList()) {
                val (code, lines) = capture(docker.path, inspectHealthArgs(containerName(project, svc))) ?: (1 to emptyList())
                if (code == 0 && lines.firstOrNull()?.trim() == "healthy") pending -= svc
            }
            if (pending.isEmpty()) break
            if (System.currentTimeMillis() > deadline) {
                throw PhaseError(ProvisionPhase.wait, "${pending.first()} unhealthy after budget — see log")
            }
            delay(2_000)
        }
    }

    /** Tries each candidate until the primary accepts createUser (PRV-BOOT-5). */
    private suspend fun createRootWalking(
        labId: String,
        docker: io.mex.backup.ToolInfo,
        project: String,
        services: List<String>,
        password: String,
    ) {
        for (svc in services) {
            val out = mongosh(labId, docker, containerName(project, svc), createRootScript(password))
            if ("CREATED" in out) return
            if ("NOT_PRIMARY" !in out) {
                throw PhaseError(ProvisionPhase.auth, "createUser on $svc failed — see log")
            }
        }
        throw PhaseError(ProvisionPhase.auth, "no primary accepted createUser")
    }

    /** Host-side verification through the to-be-registered URI; null = ok (PRV-BOOT-6). */
    private suspend fun verifyLab(lab: Lab, uri: String): String? {
        var last: String? = null
        repeat(5) { attempt ->
            try {
                shortClient(uri).use { client ->
                    val admin = client.getDatabase("admin")
                    admin.runCommand(Document("ping", 1))
                    when (val t = lab.topology) {
                        is LabTopology.ReplicaSet -> {
                            val st = admin.runCommand(Document("replSetGetStatus", 1))
                            val members = (st["members"] as? List<*>)?.size ?: 0
                            if (members != t.members) {
                                throw RuntimeException("expected ${t.members} members, saw $members")
                            }
                        }
                        is LabTopology.Sharded -> {
                            val shards = client.getDatabase("config").getCollection("shards").countDocuments()
                            if (shards.toInt() != t.shards) {
                                throw RuntimeException("expected ${t.shards} shards, saw $shards")
                            }
                        }
                        is LabTopology.Standalone -> Unit
                    }
                }
                return null
            } catch (e: Exception) {
                last = e.message ?: e::class.java.simpleName
                if (attempt < 4) runCatching { Thread.sleep(2_000) }
            }
        }
        return last
    }

    private fun verifyPing(uri: String): String? = try {
        shortClient(uri).use { it.getDatabase("admin").runCommand(Document("ping", 1)) }
        null
    } catch (e: Exception) {
        e.message ?: e::class.java.simpleName
    }

    private fun shortClient(uri: String): MongoClient = MongoClients.create(
        MongoClientSettings.builder()
            .applicationName("mongo-explorer-v3-lab")
            .applyConnectionString(ConnectionString(uri))
            .applyToClusterSettings { it.serverSelectionTimeout(5, TimeUnit.SECONDS) }
            .applyToSocketSettings { it.connectTimeout(5, TimeUnit.SECONDS) }
            .build(),
    )

    private fun capture(binary: String, args: List<String>): Pair<Int, List<String>>? = runCatching {
        val p = ProcessBuilder(listOf(binary) + args).redirectErrorStream(true).start()
        val lines = p.inputStream.bufferedReader().readLines()
        if (!p.waitFor(10, TimeUnit.SECONDS)) {
            p.destroy()
            return@runCatching null
        }
        p.exitValue() to lines
    }.getOrNull()

    private fun writeOwnerOnly(path: Path, content: String) {
        Files.writeString(path, content)
        runCatching {
            Files.setPosixFilePermissions(
                path,
                setOf(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE),
            )
        }
    }
}
