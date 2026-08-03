package io.mex.provision.k8s

import io.mex.backup.ToolInfo
import io.mex.backup.ToolProcess
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.net.InetAddress
import java.net.ServerSocket

/**
 * Supervised `kubectl port-forward` (K8P-CONN-2). Forwards die routinely — pod restarts,
 * API-server hiccups, laptop sleep — so the session restarts with backoff and reports an
 * honest down state after three failures rather than pretending to be connected.
 * Sessions live in-process only; nothing auto-starts at boot (K8P-CONN-4).
 */
class ForwardSession(
    private val tool: ToolInfo,
    private val target: KubeTarget,
    private val namespace: String,
    private val resource: String,
    private val scope: CoroutineScope,
    private val onState: (ForwardState) -> Unit,
    private val onLog: (String) -> Unit,
) {
    @Volatile
    private var proc: ToolProcess? = null

    @Volatile
    private var stopped = false

    val localPort: Int = reserveLoopbackPort()

    fun start() {
        stopped = false
        scope.launch { supervise() }
    }

    fun stop() {
        stopped = true
        proc?.cancel()
        onState(ForwardState.Down("stopped"))
    }

    private suspend fun supervise() {
        var attempt = 0
        while (!stopped && attempt < 4) {
            if (attempt > 0) {
                val backoff = listOf(1_000L, 3_000L, 9_000L)[minOf(attempt - 1, 2)]
                onLog("port-forward restarting in ${backoff / 1000}s (attempt ${attempt + 1}/4)")
                delay(backoff)
                if (stopped) return
            }
            onState(ForwardState.Starting)
            var exitCode: Int? = null
            val p = ToolProcess(
                binary = tool.path,
                args = portForwardArgs(target, resource, localPort, 27017, namespace),
                scope = scope,
                onLine = { line ->
                    onLog(line)
                    // kubectl announces the bound address once the tunnel is live.
                    if ("Forwarding from" in line) onState(ForwardState.Up(localPort))
                },
                onExit = { code -> exitCode = code },
            )
            proc = p
            p.start()
            // Wait for the process to exit; the state callback already reported Up.
            while (exitCode == null && !stopped) delay(300)
            if (stopped) return
            attempt++
        }
        if (!stopped) onState(ForwardState.Down("port-forward failed after 4 attempts"))
    }

    private fun reserveLoopbackPort(): Int =
        ServerSocket(0, 1, InetAddress.getLoopbackAddress()).use { it.localPort }
}

sealed interface ForwardState {
    data object Starting : ForwardState
    data class Up(val localPort: Int) : ForwardState
    data class Down(val reason: String) : ForwardState
}

/** Registered URI for a forwarded deployment (K8P-CONN-1/3). */
fun k8sUri(
    localPort: Int,
    user: String,
    password: String,
    directConnection: Boolean,
    caFile: String?,
): String {
    val params = buildList {
        if (directConnection) add("directConnection=true")
        add("authSource=admin")
        if (caFile != null) {
            add("tls=true")
            add("tlsCAFile=$caFile")
            // The forward presents itself as 127.0.0.1, which is never in the cert SANs.
            add("tlsAllowInvalidHostnames=true")
        }
    }
    return "mongodb://$user:$password@127.0.0.1:$localPort/?" + params.joinToString("&")
}
