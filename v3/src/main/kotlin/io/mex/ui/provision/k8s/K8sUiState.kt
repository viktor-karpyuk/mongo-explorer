package io.mex.ui.provision.k8s

import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateMapOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import io.mex.data.K8sDeployStatus
import io.mex.provision.k8s.DeployRunner
import io.mex.provision.k8s.ForwardState
import io.mex.provision.k8s.K8sEvent
import io.mex.ui.connections.ConnectionsViewModel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

private const val LOG_TAIL = 400

/**
 * App-lifetime holder for k8s deployment events — same contract as the Docker side:
 * applies run for minutes, so the Done event (carrying the show-once root password)
 * must not depend on the view being on screen. Logs batch at 10 Hz.
 */
class K8sUiState(val runner: DeployRunner) {
    val logs = mutableStateMapOf<String, List<String>>()
    val busy = mutableStateMapOf<String, String>()
    val forwards = mutableStateMapOf<String, ForwardState>()

    var pendingResult by mutableStateOf<K8sEvent.Done?>(null)
        private set

    var epoch by mutableStateOf(0)
        private set

    fun consumeResult() {
        pendingResult = null
    }

    suspend fun bind(connectionsVm: ConnectionsViewModel) = coroutineScope {
        launch {
            runner.lifecycle.collect { ev ->
                when (ev) {
                    is K8sEvent.OpStarted -> busy[ev.id] = ev.op
                    is K8sEvent.Forward -> forwards[ev.id] = ev.state
                    is K8sEvent.Done -> {
                        busy.remove(ev.id)
                        if (ev.rowRemoved) {
                            logs.remove(ev.id)
                            forwards.remove(ev.id)
                        }
                        epoch++
                        connectionsVm.reload()
                        if (ev.status == K8sDeployStatus.ready && ev.uri != null) pendingResult = ev
                    }
                    is K8sEvent.Log -> Unit
                }
            }
        }
        val pending = HashMap<String, MutableList<String>>()
        launch {
            runner.logs.collect { ev -> pending.getOrPut(ev.id) { mutableListOf() }.add(ev.line) }
        }
        launch {
            while (true) {
                delay(100)
                if (pending.isEmpty()) continue
                for ((id, lines) in pending) {
                    logs[id] = (logs[id].orEmpty() + lines).takeLast(LOG_TAIL)
                }
                pending.clear()
            }
        }
    }
}
