package io.mex.provision.k8s

import io.mex.AppContext
import io.mex.backup.ToolInfo
import io.mex.backup.ToolProcess
import io.mex.data.ConnectionInput
import io.mex.data.K8sDeploySpec
import io.mex.data.K8sDeployStatus
import io.mex.data.K8sDeployment
import io.mex.data.K8sProfile
import io.mex.data.K8sTopology
import io.mex.mongo.MongoRegistry
import io.mex.provision.generatePassword
import io.mex.provision.redact
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
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
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermissions
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.resume

sealed class K8sEvent {
    abstract val id: String

    data class Log(override val id: String, val line: String) : K8sEvent()
    data class OpStarted(override val id: String, val op: String) : K8sEvent()
    data class Forward(override val id: String, val state: ForwardState) : K8sEvent()
    data class Done(
        override val id: String,
        val status: K8sDeployStatus,
        val error: String? = null,
        val password: String? = null,
        val uri: String? = null,
        val name: String? = null,
        val connectionId: String? = null,
        val rowRemoved: Boolean = false,
    ) : K8sEvent()
}

/**
 * Apply / watch / teardown for Kubernetes deployments (K8P-CR-3/4, K8P-TEAR).
 * Same skeleton as the Docker ProvisionRunner: one operation per deployment, lifecycle
 * events on a lossless channel, logs on a lossy one, everything on the IO dispatcher.
 */
class DeployRunner(private val ctx: AppContext, private val registry: MongoRegistry) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val ops = ConcurrentHashMap<String, Job>()
    private val secrets = ConcurrentHashMap<String, String>()
    private val forwards = ConcurrentHashMap<String, ForwardSession>()

    private val _logs = MutableSharedFlow<K8sEvent.Log>(extraBufferCapacity = 1024)
    val logs: SharedFlow<K8sEvent.Log> = _logs

    private val lifecycleCh = Channel<K8sEvent>(Channel.UNLIMITED)
    val lifecycle: Flow<K8sEvent> = lifecycleCh.receiveAsFlow()

    private fun emit(ev: K8sEvent) {
        lifecycleCh.trySend(ev)
    }

    fun isBusy(id: String): Boolean = ops.containsKey(id)
    fun forwardPort(id: String): Int? = forwards[id]?.localPort

    fun cancel(id: String) {
        ops[id]?.cancel()
    }

    /** Creates the row and applies the bundle the user confirmed (K8P-CR-2/3). */
    fun apply(spec: K8sDeploySpec, confirmedHash: String): String {
        val row = ctx.k8sDeployments.create(spec)
        launchOp(row.id, "apply") { runApply(ctx.k8sDeployments.get(row.id)!!, confirmedHash) }
        return row.id
    }

    fun refresh(id: String) = launchOp(id, "refresh") {
        val d = ctx.k8sDeployments.get(id) ?: return@launchOp
        val tool = requireKubectl()
        val cr = readCr(tool, d)
        val mapped = mapCrStatus(d.spec.operator, cr)
        ctx.k8sDeployments.setStatus(id, mapped.status, mapped.detail, d.error)
        emit(K8sEvent.Done(id, mapped.status, name = d.spec.name))
    }

    fun teardown(id: String, releasePvcs: Boolean, alsoConnection: Boolean) =
        launchOp(id, "teardown") {
            runTeardown(ctx.k8sDeployments.get(id) ?: return@launchOp, releasePvcs, alsoConnection)
        }

    /* ===================== port-forward ===================== */

    fun connect(id: String) {
        val d = ctx.k8sDeployments.get(id) ?: return
        if (forwards.containsKey(id)) return
        val tool = findKubectl() ?: return
        val session = ForwardSession(
            tool = tool,
            target = KubeTarget(d.spec.context, d.spec.kubeconfigPath),
            namespace = d.spec.namespace,
            resource = forwardTarget(d.spec.operator, d.spec.name, d.spec.topology),
            scope = scope,
            onState = { st -> emit(K8sEvent.Forward(id, st)) },
            onLog = { line -> _logs.tryEmit(K8sEvent.Log(id, line)) },
        )
        forwards[id] = session
        session.start()
    }

    fun disconnect(id: String) {
        forwards.remove(id)?.stop()
        ctx.k8sDeployments.get(id)?.connectionId?.let { connId ->
            scope.launch { registry.disconnect(connId) }
        }
    }

    /* ===================== apply ===================== */

    private suspend fun runApply(d: K8sDeployment, confirmedHash: String) {
        val tool = requireKubectl()
        val target = KubeTarget(d.spec.context, d.spec.kubeconfigPath)
        val password = generatePassword().also { secrets[d.id] = it }

        // Fail closed before touching the cluster: what we are about to apply must be
        // byte-identical to what the user read and confirmed (K8P-CR-2).
        val previewDocs = render(d.spec)
        requireFreshPreview(confirmedHash, previewDocs)

        val applyDocs = render(d.spec, mapOf("${usersSecretName(d.spec)}/password" to password))
        val bundle = bundleText(applyDocs)

        _logs.tryEmit(K8sEvent.Log(d.id, "applying ${applyDocs.size} documents to ${d.spec.context}/${d.spec.namespace}"))
        val code = runKubectl(d.id, tool, applyStdinArgs(target), stdin = bundle)
        if (code != 0) {
            fail(d, "apply failed — see log")
            return
        }
        ctx.k8sDeployments.setApplied(
            d.id,
            bundleHash(previewDocs),
            d.spec.copy(secretFingerprints = mapOf(usersSecretName(d.spec) to sha256(password))),
        )

        // Watch the CR until it reports ready (or the budget runs out).
        val budgetMs = 15 * 60 * 1000L
        val deadline = System.currentTimeMillis() + budgetMs
        var last: CrStatus? = null
        while (System.currentTimeMillis() < deadline) {
            val cr = readCr(tool, d)
            val mapped = mapCrStatus(d.spec.operator, cr)
            if (mapped != last) {
                ctx.k8sDeployments.setStatus(d.id, mapped.status, mapped.detail)
                _logs.tryEmit(K8sEvent.Log(d.id, "status: ${mapped.detail}"))
                last = mapped
            }
            if (mapped.status == K8sDeployStatus.ready) break
            delay(5_000)
        }
        if (last?.status != K8sDeployStatus.ready) {
            // Not a failure: the operator may still converge. Report honestly and let the
            // user watch — resources stay in place either way (K8P-CR-5).
            ctx.k8sDeployments.setStatus(
                d.id,
                last?.status ?: K8sDeployStatus.pending,
                last?.detail ?: "still converging",
            )
            emit(K8sEvent.Done(d.id, last?.status ?: K8sDeployStatus.pending, name = d.spec.name))
            return
        }

        // Ready: export the CA (if any), start the forward, register the connection.
        val caFile = exportCaBundle(tool, d)
        connect(d.id)
        val port = waitForForward(d.id)
        if (port == null) {
            emit(K8sEvent.Done(d.id, K8sDeployStatus.ready, name = d.spec.name, error = "port-forward did not come up"))
            return
        }
        val uri = k8sUri(
            localPort = port,
            user = "root",
            password = password,
            directConnection = d.spec.topology is K8sTopology.ReplicaSet,
            caFile = caFile?.toString(),
        )
        val connection = ctx.connections.create(
            ConnectionInput(
                name = "${d.spec.name} (k8s)",
                uri = uri,
                notes = "Provisioned by Mongo Explorer — ${d.spec.context}/${d.spec.namespace}, " +
                    "${d.spec.profile.name} profile. Reachable while the port-forward runs.",
            ),
        )
        ctx.k8sDeployments.setConnection(d.id, connection.id)
        emit(
            K8sEvent.Done(
                d.id, K8sDeployStatus.ready,
                password = password, uri = uri,
                name = d.spec.name, connectionId = connection.id,
            ),
        )
    }

    private suspend fun waitForForward(id: String, timeoutMs: Long = 30_000): Int? {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            val port = forwards[id]?.localPort ?: return null
            // The session reports Up via events; probing the port keeps this simple.
            if (runCatching {
                    java.net.Socket("127.0.0.1", port).use { true }
                }.getOrDefault(false)
            ) {
                return port
            }
            delay(1_000)
        }
        return null
    }

    /* ===================== teardown ===================== */

    private suspend fun runTeardown(d: K8sDeployment, releasePvcs: Boolean, alsoConnection: Boolean) {
        val tool = requireKubectl()
        val target = KubeTarget(d.spec.context, d.spec.kubeconfigPath)
        ctx.k8sDeployments.setStatus(d.id, K8sDeployStatus.deleting, "tearing down")

        // 0. stop the forward before deleting anything it points at.
        forwards.remove(d.id)?.stop()

        for (step in teardownSteps(d.spec, releasePvcs)) {
            _logs.tryEmit(K8sEvent.Log(d.id, "▶ ${step.label}"))
            val args = if (step.name.startsWith("-l ")) {
                // Label-selected deletion (PVCs) uses a selector rather than a name.
                listOf("delete", step.kind, "-l", step.name.removePrefix("-l "), "-n", d.spec.namespace, "--ignore-not-found") +
                    ctxArgs(target)
            } else {
                deleteArgs(target, step.kind, step.name, d.spec.namespace.takeIf { step.kind != "namespace" }, step.waitSeconds)
            }
            val code = runKubectl(d.id, tool, args)
            if (code != 0) {
                // Abort the remaining steps — never orphan silently (K8P-TEAR-4).
                val msg = "teardown: ${step.label} failed — retry when the cluster is reachable"
                ctx.k8sDeployments.setStatus(d.id, K8sDeployStatus.failed, null, msg)
                emit(K8sEvent.Done(d.id, K8sDeployStatus.failed, error = msg, name = d.spec.name))
                return
            }
        }

        if (alsoConnection) {
            d.connectionId?.let { id ->
                registry.disconnect(id)
                ctx.connections.delete(id)
            }
        }
        runCatching { caFilePath(d.id).let { Files.deleteIfExists(it) } }
        ctx.k8sDeployments.delete(d.id)
        emit(K8sEvent.Done(d.id, K8sDeployStatus.missing, name = d.spec.name, rowRemoved = true))
    }

    /* ===================== helpers ===================== */

    private fun launchOp(id: String, opName: String, body: suspend () -> Unit) {
        val job = scope.launch(start = CoroutineStart.LAZY) {
            try {
                body()
            } catch (e: CancellationException) {
                ctx.k8sDeployments.setStatus(id, K8sDeployStatus.failed, null, "cancelled during $opName")
                emit(K8sEvent.Done(id, K8sDeployStatus.failed, error = "cancelled during $opName"))
                throw e
            } catch (e: StalePreviewException) {
                ctx.k8sDeployments.setStatus(id, K8sDeployStatus.failed, null, e.message)
                emit(K8sEvent.Done(id, K8sDeployStatus.failed, error = e.message))
            } catch (e: Exception) {
                ctx.k8sDeployments.setStatus(id, K8sDeployStatus.failed, null, e.message)
                emit(K8sEvent.Done(id, K8sDeployStatus.failed, error = e.message))
            }
        }
        if (ops.putIfAbsent(id, job) != null) {
            job.cancel()
            return
        }
        job.invokeOnCompletion {
            ops.remove(id)
            secrets.remove(id)
        }
        emit(K8sEvent.OpStarted(id, opName))
        job.start()
    }

    private fun fail(d: K8sDeployment, message: String) {
        ctx.k8sDeployments.setStatus(d.id, K8sDeployStatus.failed, null, message)
        emit(K8sEvent.Done(d.id, K8sDeployStatus.failed, error = message, name = d.spec.name))
    }

    private fun requireKubectl(): ToolInfo =
        findKubectl() ?: throw RuntimeException("kubectl not found on PATH or common locations")

    private fun readCr(tool: ToolInfo, d: K8sDeployment): String? {
        val target = KubeTarget(d.spec.context, d.spec.kubeconfigPath)
        val (code, lines) = kubectlRead(
            tool,
            getJsonArgs(target, crPlural(d.spec.operator), d.spec.name, d.spec.namespace),
        ) ?: return null
        if (code != 0) return null
        return lines.joinToString("\n").ifBlank { null }
    }

    /** Exports the cluster CA so the driver can verify TLS through the forward (K8P-CONN-3). */
    private fun exportCaBundle(tool: ToolInfo, d: K8sDeployment): Path? {
        val secret = tlsSecretName(d.spec.operator, d.spec) ?: return null
        val target = KubeTarget(d.spec.context, d.spec.kubeconfigPath)
        val args = listOf(
            "get", "secret", secret, "-n", d.spec.namespace,
            "-o", "jsonpath={.data.ca\\.crt}",
        ) + ctxArgs(target)
        val (code, lines) = kubectlRead(tool, args) ?: return null
        val b64 = lines.firstOrNull()?.trim().orEmpty()
        if (code != 0 || b64.isBlank()) return null
        val pem = runCatching { String(java.util.Base64.getDecoder().decode(b64)) }.getOrNull() ?: return null
        val path = caFilePath(d.id)
        Files.createDirectories(path.parent)
        runCatching {
            if (!Files.exists(path)) {
                Files.createFile(path, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
            }
        }
        Files.writeString(path, pem)
        return path
    }

    private fun caFilePath(id: String): Path = ctx.dataDir.resolve("k8s-ca").resolve("$id.pem")

    private fun sha256(s: String): String =
        java.security.MessageDigest.getInstance("SHA-256").digest(s.toByteArray())
            .joinToString("") { "%02x".format(it) }

    /** Streams a kubectl invocation into the log (redacted) and resumes with its exit code. */
    private suspend fun runKubectl(
        id: String,
        tool: ToolInfo,
        args: List<String>,
        stdin: String? = null,
    ): Int = suspendCancellableCoroutine { cont ->
        val secretList = listOfNotNull(secrets[id])
        val proc = ToolProcess(
            binary = tool.path,
            args = args,
            scope = scope,
            onLine = { line -> _logs.tryEmit(K8sEvent.Log(id, redact(line, secretList))) },
            onExit = { code -> if (cont.isActive) cont.resume(code) },
            stdin = stdin,
        )
        cont.invokeOnCancellation { proc.cancel() }
        proc.start()
    }
}
