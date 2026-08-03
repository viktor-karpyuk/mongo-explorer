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
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.receiveAsFlow
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

    /** Emitted the moment an operation is accepted, so the UI can show busy immediately. */
    data class OpStarted(override val labId: String, val op: String) : LabEvent()

    /**
     * [password]/[uri] are only set on a successful provision — the show-once banner
     * (PRV-SEC-3). [labName]/[connectionId] are carried so consumers never have to
     * re-join against a possibly-stale lab list. [rowRemoved] marks a completed destroy.
     */
    data class Done(
        override val labId: String,
        val status: LabStatus,
        val error: String? = null,
        val password: String? = null,
        val uri: String? = null,
        val labName: String? = null,
        val connectionId: String? = null,
        val rowRemoved: Boolean = false,
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

    /**
     * Two channels with different loss contracts: log lines are volume and may drop under
     * pressure; lifecycle events (Phase/OpStarted/Done — which carries the show-once
     * password) must never be lost, including those emitted before the UI collector binds
     * at boot, so they ride an unbounded channel drained by the single app-lifetime consumer.
     */
    private val _logs = MutableSharedFlow<LabEvent.Log>(extraBufferCapacity = 1024)
    val logs: SharedFlow<LabEvent.Log> = _logs

    private val lifecycleCh = Channel<LabEvent>(Channel.UNLIMITED)
    val lifecycle: Flow<LabEvent> = lifecycleCh.receiveAsFlow()

    private fun emitLifecycle(ev: LabEvent) {
        lifecycleCh.trySend(ev)
    }

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

    fun start(labId: String) {
        // Belt-and-braces for PRV-LIFE-5 — the UI disables Start, but flipping a stopped
        // lab to failed from inside the op would be worse than silently refusing.
        val lab = ctx.labs.get(labId) ?: return
        if (lab.appMajor > io.mex.data.LAB_APP_MAJOR) return
        launchOp(labId, "start") { runStart(ctx.labs.get(labId) ?: return@launchOp) }
    }

    fun stop(labId: String) = launchOp(labId, "stop") { runStop(ctx.labs.get(labId) ?: return@launchOp) }

    fun destroy(labId: String, alsoConnection: Boolean) =
        launchOp(labId, "destroy") { runDestroy(ctx.labs.get(labId) ?: return@launchOp, alsoConnection) }

    /** Docker is the source of truth; corrects every idle lab's row (PRV-LIFE-1/4). */
    fun reconcile() {
        scope.launch {
            // The busy set is snapshotted BEFORE `docker ps`: an op finishing between the
            // snapshot and the loop would otherwise let its stale container states clobber
            // the status the op just wrote (running↔stopped flicker).
            val busyAtSnapshot = ops.keys.toSet()
            val docker = findDocker() ?: return@launch
            val (code, lines) = capture(docker.path, psArgs()) ?: return@launch
            if (code != 0) return@launch
            val byLab = lines.mapNotNull(::parsePsLine).filter { it.labId != null }.groupBy { it.labId!! }
            for (lab in ctx.labs.list()) {
                if (lab.id in busyAtSnapshot || isBusy(lab.id)) continue
                val states = byLab[lab.id].orEmpty().map { it.state }
                val corrected = reconcileStatus(lab.status, states) ?: continue
                val error = when (corrected) {
                    LabStatus.missing -> "containers no longer exist"
                    LabStatus.failed -> "interrupted — the app closed during provisioning"
                    else -> null
                }
                ctx.labs.setStatus(lab.id, corrected, error)
                emitLifecycle(LabEvent.Done(lab.id, corrected, error, labName = lab.name))
            }
        }
    }

    /* ===================== operations ===================== */

    private fun launchOp(labId: String, opName: String, body: suspend () -> Unit) {
        // Lazy start closes two races: an op finishing before its map insert would leave a
        // completed job stuck in `ops` (lab busy forever), and reconcile() could observe the
        // row between create and insert and flip a live provision to failed.
        val job = scope.launch(start = kotlinx.coroutines.CoroutineStart.LAZY) {
            try {
                body()
            } catch (e: CancellationException) {
                recordFailure(labId, opName, "cancelled during $opName")
                throw e
            } catch (e: Exception) {
                recordFailure(labId, opName, e.message)
            }
        }
        if (ops.putIfAbsent(labId, job) != null) {
            job.cancel()
            return
        }
        job.invokeOnCompletion {
            ops.remove(labId)
            secrets.remove(labId)
        }
        emitLifecycle(LabEvent.OpStarted(labId, opName))
        job.start()
    }

    /**
     * `failed` is reserved for provisions (whose containers are kept for diagnosis and
     * whose only exit is Destroy). A stop/start hiccup — Docker Desktop restarting, an
     * app quit mid-op — must not funnel a healthy lab into that Destroy-only dead end,
     * so lifecycle failures resolve to the status Docker actually reports, with the
     * error message kept on the row.
     */
    private fun recordFailure(labId: String, opName: String, message: String?) {
        val status = if (opName == "stop" || opName == "start") {
            dockerTruth(labId) ?: LabStatus.failed
        } else {
            LabStatus.failed
        }
        ctx.labs.setStatus(labId, status, message)
        emitLifecycle(LabEvent.Done(labId, status, message))
    }

    private fun dockerTruth(labId: String): LabStatus? {
        val docker = findDocker() ?: return null
        val (code, lines) = capture(docker.path, psArgs(labId)) ?: return null
        if (code != 0) return null
        val states = lines.mapNotNull(::parsePsLine).map { it.state }
        return when {
            states.any { it == "running" || it == "restarting" } -> LabStatus.running
            states.isNotEmpty() -> LabStatus.stopped
            else -> LabStatus.missing
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
            var ports = allocatePorts(p.clientServices)
            ctx.labs.setPorts(lab.id, ports)
            val dir = Path.of(lab.dir)
            Files.createDirectories(dir)
            val password = if (lab.auth) generatePassword().also { secrets[lab.id] = it } else null
            if (needsKeyfile(lab)) writeOwnerOnly(dir.resolve("keyfile"), generateKeyfile())
            Files.writeString(dir.resolve("compose.yaml"), renderCompose(lab, ports))

            /* up — reserved ports are released before compose binds them, so another process
               can win the race; a conflict re-allocates and retries (PRV-RENDER-3). */
            phase = ProvisionPhase.up
            emitPhase(lab.id, ProvisionPhase.up)
            var upAttempt = 1
            while (true) {
                var conflict = false
                val code = runDocker(lab.id, docker, composeArgs(lab.dir, "up", "-d")) { line ->
                    if ("port is already allocated" in line || "address already in use" in line) conflict = true
                }
                if (code == 0) break
                if (!conflict || upAttempt >= 3) {
                    throw PhaseError(phase, "docker compose up failed — see log")
                }
                upAttempt++
                _logs.tryEmit(LabEvent.Log(lab.id, "port conflict — reallocating (attempt $upAttempt/3)"))
                ports = allocatePorts(p.clientServices)
                ctx.labs.setPorts(lab.id, ports)
                Files.writeString(dir.resolve("compose.yaml"), renderCompose(lab, ports))
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
            emitLifecycle(
                LabEvent.Done(
                    lab.id, LabStatus.running, password = password, uri = uri,
                    labName = lab.name, connectionId = connection.id,
                ),
            )
        } catch (e: PhaseError) {
            // Containers stay up for diagnosis; Destroy is offered, never forced (PRV-BOOT-7).
            ctx.labs.setStatus(lab.id, LabStatus.failed, e.message)
            emitLifecycle(LabEvent.Done(lab.id, LabStatus.failed, e.message, labName = lab.name))
        }
    }

    private suspend fun runStart(lab: Lab) {
        val docker = requireDocker()
        if (runDocker(lab.id, docker, composeArgs(lab.dir, "start")) != 0) {
            throw RuntimeException("docker compose start failed — a lab port may be taken; see log")
        }
        val p = plan(lab)
        awaitHealthy(lab, docker, p.mongods.map { it.name } + p.mongosList)
        // Credentials live in the registered connection; verify reachability only.
        val uri = lab.connectionId?.let { ctx.connections.get(it)?.uri }
        uri?.let { u -> verifyPing(u)?.let { throw RuntimeException("verify: $it") } }
        ctx.labs.setStatus(lab.id, LabStatus.running)
        emitLifecycle(LabEvent.Done(lab.id, LabStatus.running, labName = lab.name))
    }

    private suspend fun runStop(lab: Lab) {
        val docker = requireDocker()
        if (runDocker(lab.id, docker, composeArgs(lab.dir, "stop")) != 0) {
            throw RuntimeException("docker compose stop failed — see log")
        }
        // Stop never touches volumes; there is no keep-data flag (PRV-LIFE-2).
        ctx.labs.setStatus(lab.id, LabStatus.stopped)
        emitLifecycle(LabEvent.Done(lab.id, LabStatus.stopped, labName = lab.name))
    }

    private suspend fun runDestroy(lab: Lab, alsoConnection: Boolean) {
        val docker = findDocker()
        val composeFile = File(lab.dir, "compose.yaml")
        if (docker != null && composeFile.exists()) {
            // `down -v` tolerates already-gone containers (exit 0), so a non-zero exit means
            // the teardown genuinely did not run (daemon down). Deleting the row and compose
            // file anyway would orphan labelled containers/volumes with no record left to
            // destroy them by — keep everything and let the user retry.
            if (runDocker(lab.id, docker, composeArgs(lab.dir, "down", "-v")) != 0) {
                val msg = "destroy failed — is the Docker daemon running? Try again."
                ctx.labs.setStatus(lab.id, LabStatus.failed, msg)
                emitLifecycle(LabEvent.Done(lab.id, LabStatus.failed, msg, labName = lab.name))
                return
            }
        } else if (docker == null) {
            _logs.tryEmit(LabEvent.Log(lab.id, "docker unavailable — removing records only"))
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
        emitLifecycle(LabEvent.Done(lab.id, LabStatus.missing, labName = lab.name, rowRemoved = true))
    }

    /* ===================== helpers ===================== */

    private fun requireDocker() =
        findDocker() ?: throw RuntimeException("docker not found on PATH or common locations")

    private fun emitPhase(labId: String, phase: ProvisionPhase) {
        emitLifecycle(LabEvent.Phase(labId, phase))
    }

    /** Streams output into the event log (redacted) and resumes with the exit code. */
    private suspend fun runDocker(
        labId: String,
        docker: io.mex.backup.ToolInfo,
        args: List<String>,
        stdin: String? = null,
        sink: ((String) -> Unit)? = null,
    ): Int = suspendCancellableCoroutine { cont ->
        val secretList = listOfNotNull(secrets[labId])
        val proc = ToolProcess(
            binary = docker.path,
            args = args,
            scope = scope,
            onLine = { line ->
                sink?.invoke(line)
                _logs.tryEmit(LabEvent.Log(labId, redact(line, secretList)))
            },
            onExit = { code -> if (cont.isActive) cont.resume(code) },
            stdin = stdin,
        )
        cont.invokeOnCancellation { proc.cancel() }
        proc.start()
    }

    /**
     * Runs a mongosh script in a container and returns its full output for sentinel
     * checks. [viaStdin] keeps secret-bearing scripts out of world-readable argv.
     */
    private suspend fun mongosh(
        labId: String,
        docker: io.mex.backup.ToolInfo,
        container: String,
        script: String,
        viaStdin: Boolean = false,
    ): String {
        val out = StringBuilder()
        if (viaStdin) {
            runDocker(labId, docker, execMongoshStdinArgs(container), stdin = script) { out.appendLine(it) }
        } else {
            runDocker(labId, docker, execMongoshArgs(container, script)) { out.appendLine(it) }
        }
        return out.toString()
    }

    /**
     * Polls container health until every service is healthy (PRV-BOOT-2, PRV-NFR-1
     * budget) — one `docker ps` per tick for the whole lab, not one inspect per service.
     */
    private suspend fun awaitHealthy(lab: Lab, docker: io.mex.backup.ToolInfo, services: List<String>) {
        val project = plan(lab).project
        val expected = services.associateBy { containerName(project, it) }
        val deadline = System.currentTimeMillis() + 30_000L + 15_000L * services.size
        while (true) {
            val (code, lines) = capture(docker.path, psStatusArgs(lab.id)) ?: (1 to emptyList())
            val healthy = if (code == 0) {
                lines.filter { "(healthy" in it }.map { it.substringBefore(" ") }.toSet()
            } else emptySet()
            val pending = expected.keys - healthy
            if (pending.isEmpty()) break
            if (System.currentTimeMillis() > deadline) {
                val svc = expected[pending.first()] ?: pending.first()
                throw PhaseError(ProvisionPhase.wait, "$svc unhealthy after budget — see log")
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
            val out = mongosh(labId, docker, containerName(project, svc), createRootScript(password), viaStdin = true)
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
                // delay(), not Thread.sleep — Cancel during verify must actually cancel.
                if (attempt < 4) delay(2_000)
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

    private fun capture(binary: String, args: List<String>): Pair<Int, List<String>>? =
        io.mex.backup.runBounded(listOf(binary) + args, timeoutSec = 10)

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
