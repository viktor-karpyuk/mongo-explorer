package io.mex.ui.provision

import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateMapOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import io.mex.data.LabStatus
import io.mex.data.ProvisionPhase
import io.mex.provision.LabEvent
import io.mex.provision.ProvisionRunner
import io.mex.ui.connections.ConnectionsViewModel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

const val LOG_TAIL = 400

/**
 * App-lifetime holder for provisioning events. Provisions run for minutes; if the only
 * collector lived inside ProvisionView, navigating to another tab would drop the Done
 * event — losing the show-once root password and leaving the sidebar without the new
 * connection until an unrelated reload. Bound once from App, it outlives view switches.
 *
 * Log lines are flushed into snapshot state at ~10 Hz, not per line: `docker compose up`
 * on a cold image emits bursts of pull-progress lines, and every write to the state map
 * invalidates every lab row — per-line writes turn a pull into a recomposition storm.
 */
class ProvisionUiState(val runner: ProvisionRunner) {
    val logs = mutableStateMapOf<String, List<String>>()
    val phases = mutableStateMapOf<String, ProvisionPhase>()

    /** Set the instant an op is accepted, so buttons give feedback before the first log line. */
    val busy = mutableStateMapOf<String, String>() // labId -> op name

    /** Held until the result banner is actually displayed and dismissed (PRV-SEC-3). */
    var pendingResult by mutableStateOf<LabEvent.Done?>(null)
        private set

    /** Bumped on every terminal event so views reload their lab list. */
    var epoch by mutableStateOf(0)
        private set

    fun consumeResult() {
        pendingResult = null
    }

    /** Collect on the composition's main dispatcher — state maps are not thread-safe. */
    suspend fun bind(connectionsVm: ConnectionsViewModel) = coroutineScope {
        // Lifecycle events: lossless channel — includes anything emitted before this bind.
        launch {
            runner.lifecycle.collect { ev ->
                when (ev) {
                    is LabEvent.OpStarted -> busy[ev.labId] = ev.op
                    is LabEvent.Phase -> phases[ev.labId] = ev.phase
                    is LabEvent.Done -> {
                        busy.remove(ev.labId)
                        phases.remove(ev.labId)
                        if (ev.rowRemoved) logs.remove(ev.labId)
                        epoch++
                        connectionsVm.reload()
                        if (ev.status == LabStatus.running && ev.uri != null) pendingResult = ev
                    }
                    is LabEvent.Log -> Unit // logs ride their own flow
                }
            }
        }
        // Log lines: accumulate off-snapshot, flush at 10 Hz. Both coroutines run on the
        // same (main) dispatcher, so the pending map needs no synchronization.
        val pending = HashMap<String, MutableList<String>>()
        launch {
            runner.logs.collect { ev ->
                pending.getOrPut(ev.labId) { mutableListOf() }.add(ev.line)
            }
        }
        launch {
            while (true) {
                delay(100)
                if (pending.isEmpty()) continue
                for ((labId, lines) in pending) {
                    logs[labId] = (logs[labId].orEmpty() + lines).takeLast(LOG_TAIL)
                }
                pending.clear()
            }
        }
    }
}
